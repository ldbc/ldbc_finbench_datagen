package ldbc.finbench.datagen.util;


public class ZOrder {

    private int maxBitNum;

    public ZOrder(int maxNumBit) {
        this.maxBitNum = maxNumBit;
    }

    public int getZValue(int x, int y) {

        String sx = Integer.toBinaryString(x);
        int numberToAddX = maxBitNum - sx.length();
        for (int i = 0; i < numberToAddX; i++) {
            sx = "0" + sx;
        }

        String sy = Integer.toBinaryString(y);
        int numberToAddY = maxBitNum - sy.length();
        for (int i = 0; i < numberToAddY; i++) {
            sy = "0" + sy;
        }

        String sz = "";
        for (int i = 0; i < sx.length(); i++) {
            sz = sz + sx.substring(i, i + 1) + "" + sy.substring(i, i + 1);
        }

        return Integer.parseInt(sz, 2);
    }
}
