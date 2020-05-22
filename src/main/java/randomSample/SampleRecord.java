package randomSample;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class SampleRecord {
    private List<Character> list;
    private int streamlength;
    private int listlength;
    private int curlength;

    public SampleRecord(int size) {
        list = new LinkedList();
        streamlength = 0;
        listlength = size;
        curlength = 0;
    }

    public void setList(List list) {
        this.list = list;
    }

    public void setStreamlength(int streamlength) {
        this.streamlength = streamlength;
    }

    public void setListlength(int arraylength) {
        this.listlength = arraylength;
    }

    public void setCurlength(int curlength) {
        this.curlength = curlength;
    }

    public List getList() {
        return list;
    }

    public int getStreamlength() {
        return streamlength;
    }

    public int getListlength() {
        return listlength;
    }

    public int getCurlength() {
        return curlength;
    }

    @Override
    public String toString() {
        return "SampleRecord{" +
                "list=" + list +
                ", streamlength=" + streamlength +
                ", listlength=" + listlength +
                ", curlength=" + curlength +
                '}';
    }

    /**
     * @author ShenPotato
     * @descrpiton updateFunction
     * 1. streamlength <- streamlength + 1
     * 2. pick i uniformly from {1...streamlength}
     * 3. if i < arraylength then current[i] <- input character
     * @date 2020/5/22 8:09 下午
     */
    public SampleRecord update(Character character) {
        if (this.curlength < this.getListlength()) {
            list.add(character);
            streamlength++;
            curlength++;
        } else {
            streamlength++;
            Random random = new Random();
            int num = random.nextInt(streamlength) + 1;   // get random number from 1 to streamlength
            if (num <= listlength) {
                list.set(num - 1, character);
            }
        }
        return this;
    }

    /**
     * @return
     * @author ShenPotato
     * @descrpiton
     * @date 2020/5/22 8:13 下午
     */
    public SampleRecord merge(SampleRecord sampleRecord) {
        if (sampleRecord == null) return this;

        SampleRecord summary = new SampleRecord(listlength);
        List sampleList1 = this.list;
        List sampleList2 = sampleRecord.getList();
        int n1 = this.getStreamlength();
        int n2 = sampleRecord.getStreamlength();
        Random random = new Random();

        for (int i = 0; i < listlength; i++) {
            int num = random.nextInt(n1 + n2);
            if (num < n1) {
                num = random.nextInt(sampleList1.size());
                summary.update((Character) sampleList1.get(num));
                sampleList1.remove(num);
                --n1;
            } else {
                num = random.nextInt(sampleList2.size());
                summary.update((Character) sampleList2.get(num));
                sampleList2.remove(num);
                --n2;
            }
        }
        summary.setStreamlength(n1 + n2);
        return summary;
    }
}
