package Blog.aibeifeng.com;

import java.io.IOException;

public class Weibo {

    public static void init() throws IOException {
        WeiBoUtil.createNamespace(Contants.NAME_SPACE);
        //创建用户关系表
        //创建微博内容表
        //创建收件箱表
        WeiBoUtil.createTable(Contants.RELATION_TABLE,1,"attends","fans");
        WeiBoUtil.createTable(Contants.CONTENT_TABLE,1,"info");
        WeiBoUtil.createTable(Contants.INBOX_TABLE,100,"info");
    }

    public static void main(String[] args) throws IOException {

       // init();

        //关注
        WeiBoUtil.addAttends("1001","1002","1003");

        //被关注人发微博 （多人发微博）
        WeiBoUtil.putData(Contants.CONTENT_TABLE,"1002","info","content","dayisgood");

        WeiBoUtil.putData(Contants.CONTENT_TABLE,"1002","info","content","dayisbad");

        WeiBoUtil.putData(Contants.CONTENT_TABLE,"1003","info","content","dayissleep");

        WeiBoUtil.putData(Contants.CONTENT_TABLE,"1001","info","content","dayiswork");
        //获取被关注人的微博
        WeiBoUtil.getWeibo("1001");

        //关注已经发过微博的人

        //获取关注人的微博

        //取消关注

        //获取关注人的微博

    }
}
