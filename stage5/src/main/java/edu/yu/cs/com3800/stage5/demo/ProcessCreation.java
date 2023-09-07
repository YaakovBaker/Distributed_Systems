package edu.yu.cs.com3800.stage5.demo;
//This was a partnership between the Max Friedman and Yaakov Baker foundations
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProcessCreation {
    private static final String JAVA_CLASS_PATH = System.getProperty("java.class.path");
    private int serverCount;
    public ProcessCreation(int serverCount){
        this.serverCount = serverCount;
    }

    public Map<Long, Long> startServersInOwnJVM(){
        Map<Long, Long> pokeball = new HashMap<>();//maps server to its pid
        for(long sID = 0; sID < serverCount; sID++){
            String processPath = "java -classpath " + JAVA_CLASS_PATH + " edu/yu/cs/com3800/stage5/demo/ServerCreation " + sID;
            System.out.println(processPath);
            ProcessBuilder pBuilder = new ProcessBuilder(processPath.split(" "));
            pBuilder.inheritIO();//IO stays by my process
            try {
                Process process = pBuilder.start();
                pokeball.put(sID, process.pid());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return pokeball;
    }
} 

