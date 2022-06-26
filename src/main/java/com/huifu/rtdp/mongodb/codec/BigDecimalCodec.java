package com.huifu.rtdp.mongodb.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

import java.math.BigDecimal;

/**
 * @author shuai
 */
public class BigDecimalCodec implements Codec<BigDecimal> {

    // Note that you may not want it to be double -- choose your own type.
    @Override
    public void encode(final BsonWriter writer, final BigDecimal value, final EncoderContext encoderContext) {
        writer.writeDouble(value.doubleValue());
    }

    @Override
    public BigDecimal decode(final BsonReader reader, final DecoderContext decoderContext) {
        return new BigDecimal(reader.readDouble());
    }

    @Override
    public Class<BigDecimal> getEncoderClass() {
        return BigDecimal.class;
    }
}