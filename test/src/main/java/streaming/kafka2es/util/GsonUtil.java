package streaming.kafka2es.util;

import com.google.gson.Gson;

public class GsonUtil {
    public static Gson getGson(){
        return Holder.gson;
    }

    private static class Holder{
        private static Gson gson = new Gson();
    }
}
