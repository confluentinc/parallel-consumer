package io.confluent.parallelconsumer;

import io.confluent.csid.utils.Range;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.*;

import static io.confluent.csid.utils.JavaUtils.safeCast;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.OffsetEncoding.*;

/**
 * Always starts with failed or incomplete offsets. todo docs tail runlength?
 */
@ToString(onlyExplicitlyIncluded = true, callSuper = true)
@Slf4j
class RunLengthEncoder extends OffsetEncoderBase {

    /**
     * The current run length being counted / built
     */
    private int currentRunLengthCount;

    private int previousRelativeOffsetFromBase;

    private boolean previousRunLengthState;

    /**
     * Stores all the run lengths
     */
    @Getter
    private List<Integer> runLengthEncodingIntegers;

    private Optional<byte[]> encodedBytes = Optional.empty();

    @ToString.Include
    private final Version version; // default to new version

    private static final Version DEFAULT_VERSION = Version.v2;

    public RunLengthEncoder(long baseOffset, OffsetSimultaneousEncoder offsetSimultaneousEncoder, Version newVersion) {
        super(baseOffset, offsetSimultaneousEncoder);
        version = newVersion;

        init();
    }

    private void init() {
        runLengthEncodingIntegers = new ArrayList<>();
        runLengthOffsetPairs = new TreeSet<>();
        currentRunLengthCount = 0;
        previousRelativeOffsetFromBase = 0;
        previousRunLengthState = false;
    }

    private void reset() {
        log.debug("Resetting");
        init();
    }

    @Override
    protected OffsetEncoding getEncodingType() {
        return switch (version) {
            case v1 -> RunLength;
            case v2 -> RunLengthV2;
        };
    }

    @Override
    protected OffsetEncoding getEncodingTypeCompressed() {
        return switch (version) {
            case v1 -> RunLengthCompressed;
            case v2 -> RunLengthV2Compressed;
        };
    }

    @Override
    public void encodeIncompleteOffset(final long newBaseOffset, final int relativeOffset) {
        encodeRunLength(false, newBaseOffset, relativeOffset);
    }

    @Override
    public void encodeCompletedOffset(final long newBaseOffset, final int relativeOffset) {
        encodeRunLength(true, newBaseOffset, relativeOffset);
    }

    @Override
    public byte[] serialise() throws EncodingNotSupportedException {
//        addTail();

        int entryWidth = getEntryWidth();

        ByteBuffer runLengthEncodedByteBuffer = ByteBuffer.allocate(getSize() * entryWidth);


        //for (final Integer run : getRunLengthEncodingIntegers()) {
        for (final RunLengthEntry n : runLengthOffsetPairs) {
            Integer run = n.runLength;
            switch (version) {
                case v1 -> {
                    final short shortCastRunlength = run.shortValue();
                    if (run != shortCastRunlength)
                        throw new RunlengthV1EncodingNotSupported(msg("Runlength too long for Short ({} cast to {})", run, shortCastRunlength));
                    runLengthEncodedByteBuffer.putShort(shortCastRunlength);
                }
                case v2 -> {
                    runLengthEncodedByteBuffer.putInt(run);
                }
            }
        }

        byte[] array = runLengthEncodedByteBuffer.array();
        encodedBytes = Optional.of(array);
        return array;
    }

    private int getSize() {
        //return runLengthEncodingIntegers.size();
        return runLengthOffsetPairs.size();
    }

    /**
     * Add the dangling in flight run to the list, done before serialising
     */
//    void addTail() {
//        if (runLengthOffsetPairs.isEmpty()) {
//            addRunLength(originalBaseOffset, currentRunLengthCount, safeCast(originalBaseOffset));
//        } else {
//            RunLengthEntry previous = runLengthOffsetPairs.last();
//            int relativeOffsetFromBase = safeCast(previous.startOffset + previous.runLength - originalBaseOffset);
//            addRunLength(originalBaseOffset, currentRunLengthCount, relativeOffsetFromBase);
//        }
//    }
    private int getEntryWidth() {
        return switch (version) {
            case v1 -> Short.BYTES;
            case v2 -> Integer.BYTES;
        };
    }
//
//    @Override
//    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset) {
//asdf
//    }
//
//    @Override
//    public void encodeCompletedOffset(final long baseOffset, final long relativeOffset) {
//asdf
//    }

    @Override
    public void encodeIncompleteOffset(final long baseOffset, final long relativeOffset, final long currentHighestCompleted) {
        // noop
    }


    @Override
    public void encodeCompleteOffset(final long newBaseOffset, final long relativeOffset, final long currentHighestCompleted) {
        maybeReinitialise(newBaseOffset, currentHighestCompleted);

        encodeCompletedOffset(newBaseOffset, safeCast(relativeOffset));
    }

    @Override
    public void maybeReinitialise(final long newBaseOffset, final long currentHighestCompleted) {
        boolean reinitialise = false;

        long newLength = currentHighestCompleted - newBaseOffset;
//        if (originalLength != newLength) {
////        if (this.highestSuceeded != currentHighestCompleted) {
//            log.debug("Length of Bitset changed {} to {}",
//                    originalLength, newLength);
//            reinitialise = true;
//        }

        if (originalBaseOffset != newBaseOffset) {
            log.debug("Base offset {} has moved to {} - new continuous blocks of successful work",
                    this.originalBaseOffset, newBaseOffset);
            reinitialise = true;
        }

        if (newBaseOffset < originalBaseOffset)
            throw new InternalRuntimeError("");

        if (reinitialise) {
            reinitialise(newBaseOffset, currentHighestCompleted);
        }
    }

    private void reinitialise(final long newBaseOffset, final long currentHighestCompleted) {
//        long longDelta = newBaseOffset - originalBaseOffset;
//        int baseDelta = JavaUtils.safeCast(longDelta);
        truncateRunlengthsV2(newBaseOffset);


//        currentRunLengthCount = 0;
//        previousRelativeOffsetFromBase = 0;
//        previousRunLengthState = false;

        enable();
    }

    NavigableSet<RunLengthEntry> runLengthOffsetPairs = new TreeSet<>();
//

    @ToString
    @EqualsAndHashCode(onlyExplicitlyIncluded = true)
    static class RunLengthEntry implements Comparable<RunLengthEntry> {

        @Getter
        @EqualsAndHashCode.Include
        private long startOffset;

        @Getter
        private Integer runLength;

//        @Getter
//        private boolean succeeded;

        public RunLengthEntry(final long startOffset) {
            this(startOffset, null);
        }

        public RunLengthEntry(final long startOffset, final Integer runLength) {
            if (startOffset < 0) {
                throw new IllegalArgumentException(msg("Bad start offset {}", startOffset, runLength));
            }
            this.startOffset = startOffset;
//            this.succeeded = succeeded;
            if (runLength != null)
                setRunLength(runLength);
        }

        public void setRunLength(final int runLength) {
            if (runLength < 1) {
                throw new IllegalArgumentException(msg("Bad run length {}", runLength));
            }
            this.runLength = runLength;
        }

        /**
         * Inclusive end offset of the range this entry represents
         */
        public long getEndOffsetInclusive() {
            return startOffset + runLength - 1;
        }

        public long getEndOffsetExclusive() {
            return startOffset + runLength;
        }

        public int getRelativeStartOffsetFromBase(final long baseOffset) {
            return Math.toIntExact(startOffset - baseOffset);
        }

        public int getRelativeEndOffsetFromBase(final long baseOffset) {
            return Math.toIntExact(getRelativeStartOffsetFromBase(baseOffset) + runLength - 1);
        }

        @Override
        public int compareTo(final RunLengthEncoder.RunLengthEntry o) {
            return Long.compare(startOffset, o.startOffset);
        }

    }

    /**
     * For each run entry, see if it's below the base, if it is, drop it. Find the first run length that intersects with
     * the new base, and truncate it. Finish.
     * <p>
     * Uses cached positions so it does't have to search
     */
    void truncateRunlengthsV2(final long newBaseOffset) {
        // else nothing to truncate
        if (!runLengthOffsetPairs.isEmpty()) {

            if (runLengthOffsetPairs.size() > 2_000) {
                log.debug("length: {}", runLengthEncodingIntegers.size());
            }
//
//        {
//            // sanity
//            RunLengthEntry first = runLengthOffsetPairs.first();
//            RunLengthEntry second = runLengthOffsetPairs.higher(first);
//            if (first.getEndOffsetInclusive() + 1 != second.startOffset)
//                throw new RuntimeException("");
//        }

            RunLengthEntry intersectionRunLength = runLengthOffsetPairs.floor(new RunLengthEntry(newBaseOffset));
            if (intersectionRunLength == null)
                throw new InternalRuntimeError("Couldn't find interception point");
            else if (newBaseOffset > intersectionRunLength.getEndOffsetInclusive()) {
                // there is no intersection as the new base offset is a point beyond what our run lengths encode
                // remove all
                runLengthOffsetPairs.clear();
            } else {
                // truncate intersection run length
                long adjustedRunLength = intersectionRunLength.runLength - (newBaseOffset - intersectionRunLength.startOffset);
                intersectionRunLength.setRunLength(safeCast(adjustedRunLength));

                // truncate all runlengths before intersection point
                NavigableSet<RunLengthEntry> toTruncateFromSet = runLengthOffsetPairs.headSet(intersectionRunLength, false);
                toTruncateFromSet.clear();
            }
//
//        {
//            // sanity
//            RunLengthEntry first = runLengthOffsetPairs.first();
//            RunLengthEntry second = runLengthOffsetPairs.higher(first);
//            if (first.getEndOffsetInclusive() + 1 != second.startOffset)
//                throw new RuntimeException("");
//        }
        }

        //
        this.originalBaseOffset = newBaseOffset;
    }

    /**
     * For each run entry, see if it's below the base, if it is, drop it. Find the first run length that intersects with
     * the new base, and truncate it. Finish.
     */
    void truncateRunlengths(final int newBaseOffset) {
        int currentOffset = 0;
        if (runLengthEncodingIntegers.size() > 1000) {
            log.info("length: {}", runLengthEncodingIntegers.size());
        }
        int index = 0;
        int adjustedRunLength = -1;
        for (Integer aRunLength : runLengthEncodingIntegers) {
            currentOffset = currentOffset + aRunLength;
            if (currentOffset <= newBaseOffset) {
                // drop from new collection
            } else {
                // found first intersection - truncate
                adjustedRunLength = currentOffset - newBaseOffset;
                break; // done
            }
            index++;
        }
        if (adjustedRunLength == -1) throw new InternalRuntimeError("Couldn't find interception point");
        List<Integer> integers = runLengthEncodingIntegers.subList(index, runLengthEncodingIntegers.size());
        integers.set(0, adjustedRunLength); // overwrite with adjusted

        // swap
        this.runLengthEncodingIntegers = integers;

        //
        this.originalBaseOffset = newBaseOffset;
    }

    @Override
    public int getEncodedSize() {
        return encodedBytes.get().length;
    }

    @Override
    public int getEncodedSizeEstimate() {
        int numEntries = getSize();
//        if (currentRunLengthCount > 0)
//            numEntries = numEntries + 1;
        int entryWidth = getEntryWidth();
        int accumulativeEntrySize = numEntries * entryWidth;
        return accumulativeEntrySize;// + standardOverhead;
    }

    @Override
    public byte[] getEncodedBytes() {
        return encodedBytes.get();
    }

    private void encodeRunLength(final boolean currentIsComplete, final long newBaseOffset, final int relativeOffsetFromBase) {
        segmentOrCombinePreviousEntryIfNeeded(currentIsComplete, newBaseOffset, relativeOffsetFromBase);
    }

    private void encodeRunLengthOld(final boolean currentIsComplete, final long newBaseOffset, final int relativeOffsetFromBase) {
        boolean segmented = injectGapsWithIncomplete(currentIsComplete, newBaseOffset, relativeOffsetFromBase);
        if (segmented)
            return;

        // run length
        boolean currentOffsetMatchesOurRunLengthState = previousRunLengthState == currentIsComplete;

        //

        if (currentOffsetMatchesOurRunLengthState) {
//            currentRunLengthCount++; // no gap case
            int dynamicPrevious = getPreviousRelativeOffset(safeCast(newBaseOffset) + relativeOffsetFromBase);
            int dynamicPrevious2 = getPreviousRelativeOffset2(newBaseOffset, relativeOffsetFromBase) - 1;
            int delta = relativeOffsetFromBase - previousRelativeOffsetFromBase;
            int delta2 = relativeOffsetFromBase - dynamicPrevious2;
            int currentRunLengthCountOld = currentRunLengthCount + delta;
            int currentRunLengthCountNew = currentRunLengthCount + delta2;
            currentRunLengthCount = currentRunLengthCountNew;
        } else {
            previousRunLengthState = currentIsComplete;
            addRunLength(newBaseOffset, currentRunLengthCount, relativeOffsetFromBase);
            currentRunLengthCount = 1; // reset to 1
        }
        previousRelativeOffsetFromBase = relativeOffsetFromBase;
    }

    /**
     * @return the added entry
     */
    private RunLengthEntry addRunLength(final long newBaseOffset, final int runLength, final int relativeOffsetFromBase) {
        // v1
//        runLengthEncodingIntegers.add(runLength);

        // v2
        int offset = safeCast(newBaseOffset + relativeOffsetFromBase);
//        if (!runLengthOffsetPairs.isEmpty()) {
//            RunLengthEntry previous = runLengthOffsetPairs.last();
//            if (previous != null && offset != previous.getEndOffsetInclusive() + 1)
//                throw new IllegalArgumentException(msg("Can't add a run length offset {} that's not continuous from previous {}", offset, previous));
//        }
        RunLengthEntry entry = new RunLengthEntry(offset, runLength);
        boolean containedAlready = !runLengthOffsetPairs.add(entry);
        if (containedAlready)
            throw new InternalRuntimeError(msg("Already contained a run for offset {}", offset));
        return entry;
    }

    private boolean injectGapsWithIncomplete(final boolean currentIsComplete, final long newBaseOffset, final int relativeOffsetFromBase) {
        boolean segmented = segmentOrCombinePreviousEntryIfNeeded(currentIsComplete, newBaseOffset, relativeOffsetFromBase);
        if (segmented)
            return true;

//        boolean bothThisRecordAndPreviousRecordAreComplete = previousRunLengthState && currentIsComplete;
//        if (bothThisRecordAndPreviousRecordAreComplete) {
        int differenceold = relativeOffsetFromBase - previousRelativeOffsetFromBase - 1;
        int previousOffsetOld = previousRelativeOffsetFromBase - 1;

        int previousRelativeOffset = getPreviousRelativeOffset(safeCast(newBaseOffset) + relativeOffsetFromBase);
        int previousRelativeOffset2 = getPreviousRelativeOffset2(newBaseOffset, relativeOffsetFromBase);

        RunLengthEntry dynamicPrevious = runLengthOffsetPairs.floor(new RunLengthEntry(safeCast(newBaseOffset + relativeOffsetFromBase)));
        int previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - safeCast(newBaseOffset + currentRunLengthCount);

        // difference Between This Relative Offset And Previous Run Length Entry In Run Length Sequence
        int difference = relativeOffsetFromBase - previousOffset;

        if (currentRunLengthCount == 0)
            differenceold++;

        //
        if (difference > 0) {
            // check for gap - if there's a gap, we need to assume all in-between are incomplete, except the first
            // If they don't exist, this action has no affect, as we only use it to skip succeeded

            // if we already have an ongoing run length, add it first
            if (currentRunLengthCount != 0) {
                addRunLength(newBaseOffset, currentRunLengthCount, previousOffset - currentRunLengthCount + 1);
            }

            //
            // there is a gap, so first insert the incomplete
            addRunLength(newBaseOffset, difference, relativeOffsetFromBase - difference);
            currentRunLengthCount = 1; // reset to 1
            previousRunLengthState = true; // make it no flip
            previousRelativeOffsetFromBase = relativeOffsetFromBase;
        }
//        }
        return segmented;
    }

    private boolean segmentOrCombinePreviousEntryIfNeeded(final boolean currentIsComplete, final long newBaseOffset, final int relativeOffsetFromBase) {
        if (!currentIsComplete) {
            throw new InternalRuntimeError("Entries being added should always be complete, as the range by definition starts out incomplete. We never add incompletes because things never transition from complete to incomplete.");
        }
        RunLengthEntry intersectingWith = runLengthOffsetPairs.floor(new RunLengthEntry(newBaseOffset + relativeOffsetFromBase));
        if (intersectingWith == null) {
            // first entry
            addRunLength(newBaseOffset, relativeOffsetFromBase, 0); // derived incompletes
            addRunLength(newBaseOffset, 1, relativeOffsetFromBase); // this complete
            return false;
        }

        long offset = newBaseOffset + relativeOffsetFromBase;
        boolean segmented = false;
        boolean willInteractWithAnExistingRun = offset >= intersectingWith.startOffset && offset < intersectingWith.getEndOffsetExclusive();
        if (willInteractWithAnExistingRun) {
            // segment

            RunLengthEntry next = runLengthOffsetPairs.higher(intersectingWith);

            segmented = true;

            if (intersectingWith.runLength == 1) {
                // simple path
                // single bad entry to be replaced, but with 2 good entry neighbors - combine all three
                RunLengthEntry previous = runLengthOffsetPairs.lower(intersectingWith);
                int newRun = 0;
                if (previous != null) {
                    int runDown = previous.runLength;
                    newRun = newRun + runDown;
                }
                int runUp = next.runLength;
                newRun = newRun + 1 + runUp;
                runLengthOffsetPairs.remove(previous);
                runLengthOffsetPairs.remove(intersectingWith);
                runLengthOffsetPairs.remove(next);
                runLengthOffsetPairs.add(new RunLengthEntry(intersectingWith.startOffset, newRun));
            } else {

                // remove the old entry which must have been incompletes
                boolean missing = !runLengthOffsetPairs.remove(intersectingWith);
                if (missing)
                    throw new InternalRuntimeError("Cant find element that previously existed");

                int newRunCumulative = 1;
                Integer offsetStartRelative = null;

                // create the three to replace the intersected node - 1 incomplete, 1 complete (this one), 1 incomplete
                int firstRun = safeCast(offset - intersectingWith.startOffset);
                Integer middleRelativeOffset = null;
                int firstRelativeOffset = intersectingWith.getRelativeStartOffsetFromBase(originalBaseOffset);

                if (firstRun > 0) {
                    // large gap to fill
//                RunLengthEntry runLengthEntry = new RunLengthEntry(intersectingWith.startOffset, firstRun);
                    RunLengthEntry first = addRunLength(newBaseOffset, firstRun, firstRelativeOffset);
                    middleRelativeOffset = first.getRelativeStartOffsetFromBase(originalBaseOffset) + first.runLength;
                } else {
                    // combine with the neighbor as there's no gap
                    // check for a lower neighbour
                    RunLengthEntry previous = runLengthOffsetPairs.lower(intersectingWith);
                    if (previous != null && previous.getEndOffsetExclusive() == offset) {
                        // lower neighbor connects - combine
                        newRunCumulative = newRunCumulative + previous.runLength;
                        offsetStartRelative = previous.getRelativeStartOffsetFromBase(newBaseOffset);
                        runLengthOffsetPairs.remove(previous);
                    }
                }

                if (middleRelativeOffset == null)
                    middleRelativeOffset = firstRelativeOffset;

                if (next != null) {
                    int gapUpward = next.getRelativeStartOffsetFromBase(newBaseOffset) - middleRelativeOffset;
                    if (gapUpward > 1) {
                        // there is a large gap between this success and the next
                        // add this single entry now then, and then add gap filler
//                    if (offsetStartRelative == null)
//                        offsetStartRelative = next.getRelativeStartOffsetFromBase(newBaseOffset) - 1; // shift left one place
//                    newRunCumulative = newRunCumulative + 1;

                        if (offsetStartRelative == null)
                            offsetStartRelative = relativeOffsetFromBase;

                        RunLengthEntry middle = addRunLength(newBaseOffset, newRunCumulative, offsetStartRelative);

                        // add incomplete filler
                        int lastRange = safeCast(intersectingWith.getEndOffsetInclusive() - offset);
                        if (lastRange > 0) {
                            addRunLength(newBaseOffset, lastRange, middleRelativeOffset + 1);
//                        int fillerStart = middle.getRelativeEndOffsetFromBase(newBaseOffset);
//                        int use = (fillerStart != middleRelativeOffset + 1)?
//                        if (fillerStart != middleRelativeOffset + 1) {
//                            log.trace("");
//                        }
//                        addRunLength(newBaseOffset, lastRange, fillerStart);
                        }
                    } else if (gapUpward == 1) {
                        // combine with upper
                        newRunCumulative = newRunCumulative + next.runLength; // expand
//                next.setRunLength(newRunLength);
                        runLengthOffsetPairs.remove(next);
                        if (offsetStartRelative == null)
                            offsetStartRelative = next.getRelativeStartOffsetFromBase(newBaseOffset) - 1; // shift left one place if not already established
                        RunLengthEntry end = addRunLength(newBaseOffset, newRunCumulative, offsetStartRelative);
                    } else {
                        throw new InternalRuntimeError("Invalid gap {}", gapUpward);
                    }
                }
            }
            // have to check upward too
//            checkGapDownward(newBaseOffset, relativeOffsetFromBase);
        } else {
            // new entry higher than any existing
            checkGapDownward(newBaseOffset, relativeOffsetFromBase);
        }
        return segmented;
    }

    private void checkGapDownward(final long newBaseOffset, final int relativeOffsetFromBase) {
        // extending the range
        // is there a gap?
        RunLengthEntry previous = runLengthOffsetPairs.floor(new RunLengthEntry(newBaseOffset + relativeOffsetFromBase));
        int previousRelativeEnd = previous.getRelativeEndOffsetFromBase(originalBaseOffset);
        int gap = relativeOffsetFromBase - previousRelativeEnd;
        if (gap > 1) {
            // extend and fill gap
            // add incompletes
            int newRelativeOffset = previousRelativeEnd + 1;
            int run = gap - 1;
//            int gapEntryPosition = previous.getRelativeStartOffsetFromBase(newBaseOffset);
            RunLengthEntry addedGapFiller = addRunLength(newBaseOffset, run, newRelativeOffset);
            // add this
            RunLengthEntry thisEntry = addRunLength(newBaseOffset, 1, relativeOffsetFromBase); // this complete
        } else if (gap == 1) {
            // there's no gap between this and last completed, so combine
            RunLengthEntry toCombineWithCurrent = runLengthOffsetPairs.last();
            int newExtendedRunLength = toCombineWithCurrent.getRunLength() + 1;
            toCombineWithCurrent.setRunLength(newExtendedRunLength);
        } else if (gap == 0) {
            throw new InternalRuntimeError("Invalid gap {}", gap);
        } else {
            throw new InternalRuntimeError("Invalid gap {}", gap);
        }
    }

    private int getPreviousRelativeOffset(final int offset) {
        RunLengthEntry dynamicPrevious = runLengthOffsetPairs.floor(new RunLengthEntry(offset));
        int previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - safeCast(originalBaseOffset);
        return previousOffset;
    }

    private int getPreviousRelativeOffset2(final long newBaseOffset, final int offset) {
        RunLengthEntry dynamicPrevious = runLengthOffsetPairs.floor(new RunLengthEntry(offset));
        int previousOffset = (dynamicPrevious == null) ? 0 : dynamicPrevious.runLength - safeCast(originalBaseOffset);
        return previousOffset - safeCast(newBaseOffset + currentRunLengthCount);
    }

    /**
     * @return the offsets which are succeeded
     */
    public List<Long> calculateSucceededActualOffsets() {
        List<Long> successfulOffsets = new ArrayList<>();
        boolean succeeded = false;
//        int previous = 0;
        long offsetPosition = originalBaseOffset;
        //for (final Integer run : runLengthEncodingIntegers) {
        for (final RunLengthEntry n : runLengthOffsetPairs) {
            int run = n.getRunLength();
            if (succeeded) {
//                offsetPosition++;
                for (final Integer integer : Range.range(run)) {
                    long newGoodOffset = offsetPosition + integer;
                    successfulOffsets.add(newGoodOffset);
                }
            }
//            else {
//                offsetPosition = offsetPosition + run;
//            }

            //
            offsetPosition += run;
//            previous = run;
            succeeded = !succeeded;
        }
        return successfulOffsets;
    }

}
