package com.github.zyz.kafka.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author zyz
 * @since 2018/5/27
 *
 *  Zookeeper 原生api实现LeaderElection 公平模式
 *
 *  用Zookeeper原生提供的SDK实现“先到先得——公平模式”的Leader Election。即各参与方都注册ephemeral_sequential节点，ID较小者为leader
 *  1.每个竞选失败的参与方，只能watch前一个
 *  2.要能处理部分参与方等待过程中失败的情况
 *  3.实现2个选举方法，一个阻塞直到获得leadership，另一个竞选成功则返回true，否则返回失败，但要能支持回调
 */
public class ZkLeaderElectionFair implements Watcher {

    private NodeInfo nodeInfo;
    private ZooKeeper zooKeeper;
    private final String rootPath;
    private final ElectionCallback callback;
    private final CountDownLatch leaderLatch = new CountDownLatch(1);


    public ZkLeaderElectionFair(ZooKeeper zooKeeper, String rootPath, ElectionCallback callback) {
        this.zooKeeper = zooKeeper;
        this.rootPath = rootPath;
        this.callback = callback;
        initRootPath();
    }

    public ZkLeaderElectionFair(String connectString, int sessionTimeout, String rootPath, ElectionCallback callback) {
        this.rootPath = rootPath;
        this.callback = callback;
        initZk(connectString,sessionTimeout);
        initRootPath();
    }

    private void initZk(String connectString, int sessionTimeout) {
        try {
            CountDownLatch initZkLatch = new CountDownLatch(1);
            zooKeeper = new ZooKeeper(connectString, sessionTimeout, event -> {
                switch (event.getState()) {
                    case SyncConnected:
                        System.err.println(event);
                        initZkLatch.countDown();
                        break;
                    case Disconnected:
                        System.err.println("Disconnected");
                        break;
                    default:
                        break;
                }
            });
            initZkLatch.await(10, TimeUnit.SECONDS);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void initRootPath() {
        if (exists(rootPath, null)) {
            System.err.format("Path %s already exists\n", rootPath);
        } else {
            String path = createNode(rootPath, null, CreateMode.PERSISTENT);
            System.err.format("Create Root Path %s success\n", path);
        }
    }

    private boolean exists(String path, Watcher watcher) {
        try {
            Stat stat = zooKeeper.exists(path, watcher);
            return stat != null;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    private String createNode(String path,String data,CreateMode createMode) {
        String rePath = null;
        try {
            byte[] value = data == null ? null : data.getBytes(Charset.forName("UTF-8"));
            rePath = zooKeeper.create(path, value, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return rePath;
    }

    private void deleteNode(String path) {
        try {
            zooKeeper.delete(path, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    private List<String> getChildren(String path) {
        List<String> children = null;
        try {
            children = zooKeeper.getChildren(path, false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return children;
    }


    /**
     * Watch
     * @param event Watch事件
     */
    @Override
    public void process(WatchedEvent event) {
        if (Event.EventType.NodeDeleted.equals(event.getType())) {
            System.err.format("节点 %s 被删除或者宕机\n", event.getPath());
            String nodePath = nodeInfo.getNodePath();
            System.err.format("节点 %s 收到删除通知\n", nodePath);
            if (!event.getPath().equals(nodePath)) {
                if (!exists(nodePath, null)) {
                    System.err.format("节点 %s 已经删除或者宕机, 不需要选举\n",nodePath);
                    leaderLatch.countDown();
                } else {
                    try {
                        leaderElection(true);
                    } catch (KeeperException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    /**
     * 实际的领导选举
     * @param isWatchHandler 是否在watch内处理的领导选举
     * @return 成功/失败
     * @throws KeeperException
     * @throws InterruptedException
     */
    private boolean leaderElection(boolean isWatchHandler) throws KeeperException, InterruptedException {
        boolean isSuccess = false;
        String nodePath = nodeInfo.getNodePath();
        System.err.format("节点 %s 开始选举领导者\n", nodePath);
        List<NodeInfo> childNodeInfo = makeChildNodeInfo();
        NodeInfo minNodeInfo = childNodeInfo.get(0);//排序后，第一个既是最小的节点
        if (this.nodeInfo.getId().equals(minNodeInfo.getId())) {
            if (callback == null && isWatchHandler) {
                leaderLatch.countDown();
            } else if (callback != null) {
                System.err.format("非阻塞模式，节点 %s 成为领导者,开始回调\n",nodePath);
                callback.becomeLeader(nodePath);
            }
            System.err.format("节点 %s 成为领导者\n", nodePath);
            isSuccess = true;
        } else {
            String prevWatchPath = getPrevPath(childNodeInfo);
            System.err.format("节点 %s 比childNode中最小的节点 %s 大, 开始监听前一个节点 %s\n", nodePath, minNodeInfo.getNodePath(), prevWatchPath);
            exists(prevWatchPath, this);
            if (callback == null && isWatchHandler) System.err.format("节点 %s 继续等待", nodePath); //只为了输出语句，不影响功能
            if (callback == null && !isWatchHandler) {
                System.err.format("阻塞模式，节点 %s 开始等待\n", nodePath);
                leaderLatch.await();//阻塞等待watch得到通知并再次选举leader成功
                //得到通知后，还需要再判断下nodePath是否存在，因为宕机的节点也会收到删除节点通知
                isSuccess = exists(nodePath, null);
            }
        }
        return isSuccess;
    }


    /**
     * 节点字符串处理返回顺序字符串
     *
     * @param path /leaderRoot/0000000103
     * @return  0000000103
     */
    private String getPathSeqNumber(String path) {
        return path.substring(path.lastIndexOf("/") + 1, path.length());
    }

    /**
     * 获取childNode中的上一节点
     * @param childNodeInfo childNode
     * @return /leaderRoot/0000000102
     */
    private String getPrevPath(List<NodeInfo> childNodeInfo) {
        //利用NodeInfo的equals判断
        int index = childNodeInfo.indexOf(nodeInfo);
        if (index > 0)
            return childNodeInfo.get(index - 1).getNodePath(); //获取前一个path
        throw new RuntimeException("getPrevPath error");
    }

    /**
     * 本机生成的node信息
     */
    private void makeNodeInfo() {
        String nodePath = createNode(rootPath + "/", null, CreateMode.EPHEMERAL_SEQUENTIAL);
        Integer id = Integer.parseInt(getPathSeqNumber(nodePath));
        nodeInfo = new NodeInfo(id,nodePath);
    }

    /**
     * 根节点rootPath下的子节点信息
     *
     * @return childNode 排序集合
     */
    private List<NodeInfo> makeChildNodeInfo() {
        List<String> childNode = getChildren(rootPath);
        List<NodeInfo> childNodeInfo = new ArrayList<>(childNode.size());

        for (String childNodePath : childNode) {
            Integer id = Integer.parseInt(getPathSeqNumber(childNodePath));
            String nodePath = rootPath + "/" + childNodePath;
            childNodeInfo.add(new NodeInfo(id, nodePath));
        }
        //从小到大排序
        childNodeInfo.sort((o1, o2) -> o1.getId().compareTo(o2.getId()));
        return childNodeInfo;
    }

    /**
     * 选举领导者方法
     * @param isBlocking true 阻塞直到成功成为领导者 false 不管是否成功,马上返回,成为领导者后有回调方法执行
     * @return 是否选举成功
     */
    public boolean start(boolean isBlocking) {
        //通过是否设置callback不优雅,可以优化
        if (isBlocking && Objects.nonNull(callback))
            throw new RuntimeException("使用阻塞模式, ElectionCallback必须为null");
        else if (!isBlocking)
            Objects.requireNonNull(callback, "ElectionCallback必须不为空");
        try {
            makeNodeInfo();
            return leaderElection(false);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 放弃领导者选举
     * @return 是否成功
     */
    public boolean stop() {
        if (nodeInfo != null) {
           deleteNode(nodeInfo.getNodePath());
            return true;
        }
        return false;
    }

}
