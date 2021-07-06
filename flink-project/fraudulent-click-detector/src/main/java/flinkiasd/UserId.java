package flinkiasd;

public class UserId {
    public String uid;

    public UserId() {

    }
    public UserId(String uid) {
        this.uid = uid;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {

        return uid;
        /*
        return "Alert{" +
                "uid='" + uid + '\'' +
                '}';

         */
    }
}
