package com.huifu.rtdp.mongodb.codec;

import org.bson.Transformer;

import java.math.BigDecimal;

/**
 * @author shuai
 */
public class BigDecimalTransformer implements Transformer {

	@Override
	public Object transform(Object objectToTransform) {
		BigDecimal value = (BigDecimal) objectToTransform;
    
    //Again, you may not want a double.
		return value.doubleValue();
	}
}