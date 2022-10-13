package io.confluent.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

import static io.confluent.csid.utils.StringUtils.msg;

/**
 * This implementation can force a certain encoding, useful for testing with a deterministic state
 *
 * @author Antony Stubbs
 */
@Slf4j
public class ForcedOffsetSimultaneousEncoder extends OffsetSimultaneousEncoder {

    // todo store the forced settings locally instead of in module?
    private final PCModuleTestEnv pcModuleTestEnv;

    public ForcedOffsetSimultaneousEncoder(PCModuleTestEnv pcModuleTestEnv, long baseOffsetForPartition, long highestSucceeded, SortedSet<Long> incompleteOffsets) {
        super(baseOffsetForPartition, highestSucceeded, incompleteOffsets);
        this.pcModuleTestEnv = pcModuleTestEnv;
    }

    @Override
    protected void registerEncodings(final Set<? extends OffsetEncoder> encoders) {
        super.registerEncodings(encoders);
        if (pcModuleTestEnv.compressionForced) {
            encoders.forEach(OffsetEncoder::registerCompressed);
        }
    }

    @Override
    public byte[] packSmallest() throws NoEncodingPossibleException {
        if (pcModuleTestEnv.getForcedCodec().isPresent()) {
            Optional<OffsetEncoding> forcedCodec = pcModuleTestEnv.getForcedCodec();
            OffsetEncoding forcedOffsetEncoding = forcedCodec.get();
            log.debug("Forcing use of {}, for testing", forcedOffsetEncoding);


            byte[] bytes = getEncodingMap().get(forcedOffsetEncoding);
            if (bytes == null)
                throw new NoEncodingPossibleException(msg("Can't force an encoding that hasn't been run: {}", forcedOffsetEncoding));
            return packEncoding(new EncodedOffsetPair(forcedOffsetEncoding, ByteBuffer.wrap(bytes)));
        } else {
            return super.packSmallest();
        }
    }
}
