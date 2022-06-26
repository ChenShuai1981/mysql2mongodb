package com.huifu.rtdp.mongodb.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.math.BigInteger;

/**
 * @author shuai
 */
public class BigIntegerCodecProvider implements CodecProvider {

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == BigInteger.class) {
            return (Codec<T>) (new BigIntegerCodec());
        }
        return null;
    }
}
