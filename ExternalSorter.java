import java.io.*;
import java.util.*;

public class ExternalSorter {
    int fileNum;
    public ExternalSorter(String in, String out, int numBuffers, int pageSize) throws Exception {
        //in is the name of an unsorted binary file of ints
        //out is the name of the output binary file (the destination of the sorted ints)
        //numBuffers is the number of in memory page buffers available for sorting
        //pageSize is the number of ints in a page
        int segmentNum = split(in, numBuffers, pageSize);
        fileNum=segmentNum;
        mergeFiles(segmentNum,0, numBuffers-1, out);
    }
    private int split(String in, int numBuffers, int pageSize) throws IOException {
        int elementNum = numBuffers*pageSize;
        File input = new File(in);
        ArrayList<File> segments = new ArrayList<>();
        int[] elements = new int[elementNum];
        DataInputStream inputReader=new DataInputStream(new BufferedInputStream(new FileInputStream(input)));
        boolean readCompleted = false;
        int sortedFileName=0;
        while (!readCompleted) {
            int index = elements.length;
            for (int i = 0; i < elements.length && !readCompleted; i++) {
                try {
                    elements[i] = inputReader.readInt();
                } catch (Exception e) {
                    readCompleted = true;
                    index = i;
                }
            }
            if (index != 0 && elements[0] > -1) {
                Arrays.sort(elements, 0, index);
                File sortedFiles = new File(String.valueOf(sortedFileName++));
                DataOutputStream fileWriter=new DataOutputStream(new BufferedOutputStream(new FileOutputStream(sortedFiles)));
                for (int j = 0; j < index; j++)
                    fileWriter.writeInt(elements[j]);
                fileWriter.close();
                segments.add(sortedFiles);
            }
        }
        inputReader.close();
        return segments.size();
    }

    private void mergeFiles(int segNum, int mergeStartIndex, int groupSize, String out) throws Exception {
        int groupNum= (int)Math.ceil(segNum/groupSize)+1;
        if(segNum == 1) {
            File oldName = new File(String.valueOf(fileNum-1));
            File newName = new File(out);
            if(!oldName.renameTo(newName))
                System.out.println("Error: Not renamed.");
            return;
        }
        for(int i = 0; i < groupNum; i++)
            merge((mergeStartIndex+i*groupSize),groupSize, i, segNum);
        mergeFiles(groupNum,mergeStartIndex+segNum,groupSize, out);
    }
    private void merge(int mergeStartIndex, int groupSize, int groupIndex, int segNum) throws Exception {
        if((groupIndex*groupSize+groupSize)<=segNum){
            DataInputStream[] inputReader = new DataInputStream[groupSize];
            int index=0;
            for (int i = mergeStartIndex; i < (mergeStartIndex+groupSize); i++)
                inputReader[index++] = new DataInputStream(new BufferedInputStream(new FileInputStream(String.valueOf(i))));
            MinHeapNode[] minHeapNodeArr = new MinHeapNode[groupSize];
            PriorityQueue<MinHeapNode> minHeap = new PriorityQueue<>();
            int i;
            for (i = 0; i < groupSize; i++) {
                try{
                    int nextData = inputReader[i].readInt();
                    minHeapNodeArr[i] = new MinHeapNode((nextData), i);
                    minHeap.add(minHeapNodeArr[i]);
                }catch (Exception e) {
                    break;
                }
            }
            int count = 0;
            DataOutputStream outputWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(String.valueOf(fileNum++))));
            while (count != i) {
                MinHeapNode root = minHeap.poll();
                if (root != null) {
                    outputWriter.writeInt(root.element);
                    try {
                        root.element = inputReader[root.i].readInt();
                    } catch (Exception e) {
                        root.element = Integer.MAX_VALUE;
                        count++;
                    }
                    minHeap.add(root);
                }
            }
            for (int j = 0; j < groupSize; j++)
                inputReader[j].close();
            outputWriter.close();
        }else {
            int leftNum=segNum-groupIndex*groupSize;
            DataInputStream[] inputReader = new DataInputStream[leftNum];
            int index=0;
            for (int i = mergeStartIndex; i < (mergeStartIndex+leftNum); i++)
                inputReader[index++] = new DataInputStream(new BufferedInputStream(new FileInputStream(String.valueOf(i))));
            MinHeapNode[] minHeapNodeArr = new MinHeapNode[leftNum];
            PriorityQueue<MinHeapNode> minHeap = new PriorityQueue<>();
            int i;
            for (i = 0; i < leftNum; i++) {
                try{
                    int nextData = inputReader[i].readInt();
                    minHeapNodeArr[i] = new MinHeapNode(nextData, i);
                    minHeap.add(minHeapNodeArr[i]);
                }catch (Exception e) {
                    break;
                }
            }
            int count = 0;
            DataOutputStream outputWriter = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(String.valueOf(fileNum++))));
            while (count != i) {
                MinHeapNode root = minHeap.poll();
                if (root != null) {
                    outputWriter.writeInt(root.element);
                    try {
                        root.element = inputReader[root.i].readInt();
                    } catch (Exception e) {
                        root.element = Integer.MAX_VALUE;
                        count++;
                    }
                    minHeap.add(root);
                }
            }
            for (int j = 0; j < leftNum; j++)
                inputReader[j].close();
            outputWriter.close();
        }
    }
    static class MinHeapNode implements Comparable<MinHeapNode> {
        int element;
        int i;
        public MinHeapNode(int elements, int index) {
            element = elements;
            i = index;
        }
        public int compareTo(MinHeapNode n) {
            return Integer.compare(element, n.element);
        }
    }
}