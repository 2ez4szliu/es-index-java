package utils;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.Base64;

public class VectorEncoder {
    public static float[] convertBase64ToArray(String base64Str) {
        final byte[] decode = Base64.getDecoder().decode(base64Str.getBytes());
        final FloatBuffer floatBuffer = ByteBuffer.wrap(decode).asFloatBuffer();
        final float[] dims = new float[floatBuffer.capacity()];
        floatBuffer.get(dims);

        return dims;
    }

    public static String convertArrayToBase64(float[] array) {
        final int capacity = Float.BYTES * array.length;
        final ByteBuffer bb = ByteBuffer.allocate(capacity);
        for (float v : array) {
            bb.putFloat(v);
        }
        bb.rewind();
        final ByteBuffer encodedBB = Base64.getEncoder().encode(bb);

        return new String(encodedBB.array());
    }

    public static String urlBase64Encode(String url) {
            return Base64.getUrlEncoder()
                .encodeToString(url.getBytes());
    }

    public static String urlBase64Decode(String url) {
        byte[] decodedURLBytes = Base64.getUrlDecoder().decode(url);
        return new String(decodedURLBytes);
    }
}
