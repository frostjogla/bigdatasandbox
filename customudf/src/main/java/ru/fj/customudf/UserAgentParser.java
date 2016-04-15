package ru.fj.customudf;

class UserAgentParser {

    final static String UNKNOWN = "UNKNOWN";

    final static String FAMILY_MOZILLA = "Mozilla";
    final static String FAMILY_MSIE = "MSIE";
    final static String FAMILY_UNKNOWN = "Unknown";

    final static String TYPE_FIREFOX = "Firefox";
    final static String TYPE_MSIE = "MSIE";
    final static String TYPE_CHROME = "Chrome";
    final static String TYPE_SAFARI = "Safari";
    final static String TYPE_ICEWEASEL = "Iceweasel";

    final static String OS_NAME_LINUX = "Linux";
    final static String OS_NAME_CENTOS = "CentOS";
    final static String OS_NAME_UBUNTU = "Ubuntu";
    final static String OS_NAME_REDHAT = "Red Hat";
    final static String OS_NAME_SUSE = "SUSE";
    final static String OS_NAME_MINT = "Mint";
    final static String OS_NAME_FEDORA = "Fedora";
    final static String OS_NAME_WINDOWS = "Windows";
    final static String OS_NAME_SUNOS = "SunOS";
    final static String OS_NAME_IRIX = "IRIX";
    final static String OS_NAME_ANDROID = "Android";

    final static String DEVICE_MOBILE = "Mobile";
    final static String DEVICE_IPAD = "iPad";
    final static String DEVICE_IPHONE = "iPhone";
    final static String DEVICE_IPOD = "iPod";
    final static String DEVICE_ROBOT = "Robot";
    final static String DEVICE_PC = "PC";

    final static String HTTP = "http://";
    final static String DOTNET = ".NET";

    UserAgentDTO parse(String line) {
        UserAgentDTO result = new UserAgentDTO();
        result.family = getFamily(line);
        result.type = getType(line);
        result.osName = getOsName(line);
        result.device = getDevice(line);
        return result;
    }

    private String getDevice(String line) {
        if (line.contains(DEVICE_IPHONE)) {
            return DEVICE_IPHONE;
        } else if (line.contains(DEVICE_IPAD)) {
            return DEVICE_IPAD;
        } else if (line.contains(DEVICE_IPOD)) {
            return DEVICE_IPOD;
        } else if (line.contains(DEVICE_MOBILE)) {
            return DEVICE_MOBILE;
        } else if (line.contains(DOTNET)) {
            return DEVICE_PC;
        } else if (line.contains(HTTP)) {
            return DEVICE_ROBOT;
        }
        return UNKNOWN;
    }

    private String getOsName(String line) {
        if (line.contains(OS_NAME_LINUX)) {
            if (line.contains(OS_NAME_CENTOS)) {
                return OS_NAME_LINUX + ":" + OS_NAME_CENTOS;
            } else if (line.contains(OS_NAME_UBUNTU)) {
                return OS_NAME_LINUX + ":" + OS_NAME_UBUNTU;
            } else if (line.contains(OS_NAME_REDHAT)) {
                return OS_NAME_LINUX + ":" + OS_NAME_REDHAT;
            } else if (line.contains(OS_NAME_SUSE)) {
                return OS_NAME_LINUX + ":" + OS_NAME_SUSE;
            } else if (line.contains(OS_NAME_MINT)) {
                return OS_NAME_LINUX + ":" + OS_NAME_MINT;
            } else if (line.contains(OS_NAME_FEDORA)) {
                return OS_NAME_LINUX + ":" + OS_NAME_FEDORA;
            } else if (line.contains(OS_NAME_ANDROID)) {
                return OS_NAME_LINUX + ":" + OS_NAME_ANDROID;
            }
            return OS_NAME_LINUX;
        } else if (line.contains(OS_NAME_SUNOS)) {
            return OS_NAME_SUNOS;
        } else if (line.contains(OS_NAME_IRIX)) {
            return OS_NAME_IRIX;
        } else if (line.contains(OS_NAME_WINDOWS)) {
            return OS_NAME_WINDOWS;
        }
        return UNKNOWN;
    }

    private String getFamily(String line) {
        if (line.contains(FAMILY_MOZILLA)) {
            return FAMILY_MOZILLA;
        } else if (line.contains(FAMILY_MSIE)) {
            return FAMILY_MSIE;
        }
        return UNKNOWN;
    }

    private String getType(String line) {
        if (line.contains(TYPE_ICEWEASEL)) {
            return TYPE_ICEWEASEL;
        } else if (line.contains(TYPE_CHROME)) {
            return TYPE_CHROME;
        } else if (line.contains(TYPE_SAFARI)) {
            return TYPE_SAFARI;
        } else if (line.contains(TYPE_FIREFOX)) {
            return TYPE_FIREFOX;
        } else if (line.contains(TYPE_MSIE)) {
            return TYPE_MSIE;
        }
        return UNKNOWN;
    }
}
