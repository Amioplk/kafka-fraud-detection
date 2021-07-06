package flinkiasd;

public class IpAdress {
    public String ip;

    public IpAdress() {

    }
    public IpAdress(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    @Override
    public String toString() {

        return ip;
        /*
        return "Alert{" +
                "ip='" + ip + '\'' +
                '}';

         */
    }
}
