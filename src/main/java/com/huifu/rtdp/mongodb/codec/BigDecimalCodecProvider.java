package com.huifu.rtdp.mongodb.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.math.BigDecimal;

/**
 * @author shuai
 */
public class BigDecimalCodecProvider implements CodecProvider {

    @Override
    public <T> Codec<T> get(Class<T> clazz, CodecRegistry registry) {
        if (clazz == BigDecimal.class) {
            return (Codec<T>) (new BigDecimalCodec());
        }
        return null;
    }
}
