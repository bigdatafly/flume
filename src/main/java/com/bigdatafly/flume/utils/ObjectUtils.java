package com.bigdatafly.flume.utils;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;


public class ObjectUtils {

	public static byte[] ObjectToByte(Object obj) {
		byte[] bytes = null;
		try {
			// object to bytearray
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			ObjectOutputStream oo = new ObjectOutputStream(bo);
			oo.writeObject(obj);
			bytes = bo.toByteArray();
			bo.close();
			oo.close();
		} catch (Exception e) {
			System.out.println("translation" + e.getMessage());
			e.printStackTrace();
		}
		return bytes;
	}
	
	
	public String buffer2String(ByteBuffer buffer,CharsetDecoder decoder) {

		buffer.flip();

		try {
			// log.debug("size:************"+buffer.limit());
			if (buffer.limit() <= 0)
				return "";
			CharBuffer cb = decoder.decode(buffer);
			// decoder.decode(buffer, charBuffer, true);
			// charBuffer = decoder.decode(buffer);
			buffer.flip();
			// log.debug(cb.toString());
			return cb.toString();
		} catch (Exception ex) {
			ex.printStackTrace();
			return "";
		}
	}
}
