package edu.uci.ics.hyracks.hashtable;

import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;

/**
 * An entry in the table is: frameIndex, tupleIndex, nextFrame, nextOffset
 * <frameIndex, tupleIndex> is the tuple pointer
 */
public class SerializableHashTable implements ISerializableTable {

    private IntBuffer[] headers;
    private List<IntBuffer> contents = new ArrayList<IntBuffer>();
    private Map<Integer, Integer> contentToSize = new HashMap<Integer, Integer>();
    private final IHyracksStageletContext ctx;
    private final static int INT_SIZE = 4;
    private int frameCapacity = 0;
    private int currentLargestFrameIndex = 0;
    private int tupleCount = 0;
    private int headerFrameCount = 0;

    public SerializableHashTable(int tableSize, final IHyracksStageletContext ctx) {
        this.ctx = ctx;
        int frameSize = ctx.getFrameSize();
        // init headers
        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        int headerSize = tableSize * INT_SIZE * 2 / frameSize + residual;
        headers = new IntBuffer[headerSize];
        // init content frame
        IntBuffer frame = ctx.allocateFrame().asIntBuffer();
        contents.add(frame);
        contentToSize.put(0, 0);

        frameCapacity = frame.capacity();
        if (!(frameCapacity % 4 == 0))
            throw new IllegalStateException("frame capacity is illegal: " + frameCapacity);
    }

    @Override
    public void insert(int entry, TuplePointer pointer) {
        int hFrameIndex = getHeaderFrameIndex(entry);
        int hOffset = getHeaderFrameOffset(entry);
        IntBuffer frame = headers[hFrameIndex];
        if (frame == null) {
            frame = ctx.allocateFrame().asIntBuffer();
            headers[hFrameIndex] = frame;
            resetFrame(frame);
            headerFrameCount++;
        }

        int frameIndex = frame.get(hOffset);
        int offsetIndex = frame.get(hOffset + 1);
        if (frameIndex < 0) {
            insertTuple(pointer);
            int last = currentLargestFrameIndex;
            int lastSize = contentToSize.get(last);
            int lastIndex = lastSize - 4;
            frame.put(hOffset, last);
            frame.put(hOffset + 1, lastIndex);
        } else {
            if (offsetIndex < 0)
                throw new IllegalStateException("offset index cannot be less than zero!");
            IntBuffer dataFrame;
            int currentFrame = frameIndex;
            int currentOffset = offsetIndex;
            int nextFrame = frameIndex;
            int nextOffset = offsetIndex;
            while (nextFrame >= 0) {
                currentFrame = nextFrame;
                currentOffset = nextOffset;
                dataFrame = contents.get(currentFrame);
                nextFrame = dataFrame.get(currentOffset + 2);
                nextOffset = dataFrame.get(currentOffset + 3);
            }
            insertTuple(pointer);
            dataFrame = contents.get(currentFrame);
            int last = currentLargestFrameIndex;
            int lastIndex = contentToSize.get(last) - 4;
            dataFrame.put(currentOffset + 2, last);
            dataFrame.put(currentOffset + 3, lastIndex);
        }
        tupleCount++;
    }

    @Override
    public void getTuplePointer(int entry, int offset, TuplePointer tuplePointer) {
        int hFrameIndex = getHeaderFrameIndex(entry);
        int hOffset = getHeaderFrameOffset(entry);
        IntBuffer frame = headers[hFrameIndex];
        if (frame == null) {
            tuplePointer.frameIndex = -1;
            tuplePointer.tupleIndex = -1;
            return;
        }
        int frameIndex = frame.get(hOffset);
        int offsetIndex = frame.get(hOffset + 1);

        int pos = 0;
        IntBuffer dataFrame;
        int currentFrame = frameIndex;
        int currentOffset = offsetIndex;
        int nextFrame = frameIndex;
        int nextOffset = offsetIndex;
        while (nextFrame >= 0) {
            currentFrame = nextFrame;
            currentOffset = nextOffset;
            dataFrame = contents.get(currentFrame);
            nextFrame = dataFrame.get(currentOffset + 2);
            nextOffset = dataFrame.get(currentOffset + 3);
            if (pos == offset) {
                tuplePointer.frameIndex = dataFrame.get(currentOffset);
                tuplePointer.tupleIndex = dataFrame.get(currentOffset + 1);
                return;
            }
            pos++;
        }
        tuplePointer.frameIndex = -1;
        tuplePointer.tupleIndex = -1;
    }

    @Override
    public void reset() {
        for (IntBuffer frame : headers)
            if (frame != null)
                resetFrame(frame);
        for (int i = 0; i < contents.size(); i++)
            contentToSize.put(i++, 0);
        currentLargestFrameIndex = 0;
        tupleCount = 0;
    }

    @Override
    public int getFrameCount() {
        return headerFrameCount + contents.size();
    }

    public int getTupleCount() {
        return tupleCount;
    }

    @Override
    public void close() {
        for (int i = 0; i < headers.length; i++)
            headers[i] = null;
        contents.clear();
        contentToSize.clear();
        tupleCount = 0;
        currentLargestFrameIndex = 0;
    }

    private void insertTuple(TuplePointer pointer) {
        int last = currentLargestFrameIndex;
        IntBuffer lastFrame = contents.get(last);
        Integer lastSize = contentToSize.get(last);
        if (lastSize == null)
            throw new IllegalStateException("illegal");
        if (lastSize >= frameCapacity) {
            IntBuffer newFrame;
            if (currentLargestFrameIndex >= contents.size() - 1) {
                newFrame = ctx.allocateFrame().asIntBuffer();
                contents.add(newFrame);
                currentLargestFrameIndex++;
            } else {
                currentLargestFrameIndex++;
                newFrame = contents.get(currentLargestFrameIndex);
            }
            lastSize = 0;
            lastFrame = newFrame;
            contentToSize.put(currentLargestFrameIndex, lastSize);
        }
        lastFrame.put(lastSize++, pointer.frameIndex);
        lastFrame.put(lastSize++, pointer.tupleIndex);
        lastFrame.put(lastSize++, -1);
        lastFrame.put(lastSize++, -1);
        contentToSize.put(currentLargestFrameIndex, lastSize);
    }

    private void resetFrame(IntBuffer frame) {
        for (int i = 0; i < frame.capacity(); i++)
            frame.put(i, -1);
    }

    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry * 2 / frameCapacity;
        return frameIndex;
    }

    private int getHeaderFrameOffset(int entry) {
        int offset = entry * 2 % frameCapacity;
        return offset;
    }
}
