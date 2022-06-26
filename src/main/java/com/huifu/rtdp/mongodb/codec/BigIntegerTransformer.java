package com.huifu.rtdp.mongodb.codec;

import org.bson.Transformer;

import java.math.BigInteger;

/**
 * @author shuai
 */
public class BigIntegerTransformer implements Transformer {

	@Override
	public Object transform(Object objectToTransform) {
		BigInteger value = (BigInteger) objectToTransform;
    
    //Again, you may not want a double.
		return value.longValue();
	}
}