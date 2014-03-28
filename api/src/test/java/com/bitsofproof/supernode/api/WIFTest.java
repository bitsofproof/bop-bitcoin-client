package com.bitsofproof.supernode.api;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.bitsofproof.supernode.common.ECKeyPair;
import com.bitsofproof.supernode.common.ValidationException;

public class WIFTest
{
	@BeforeClass
	public static void init ()
	{
		Security.addProvider (new BouncyCastleProvider ());
	}

	private static final String WIF = "WIF.json";

	private JSONArray readObjectArray (String resource) throws IOException, JSONException
	{
		InputStream input = this.getClass ().getResource ("/" + resource).openStream ();
		StringBuffer content = new StringBuffer ();
		byte[] buffer = new byte[1024];
		int len;
		while ( (len = input.read (buffer)) > 0 )
		{
			byte[] s = new byte[len];
			System.arraycopy (buffer, 0, s, 0, len);
			content.append (new String (buffer, "UTF-8"));
		}
		return new JSONArray (content.toString ());
	}

	@Test
	public void wifTest () throws IOException, JSONException, ValidationException
	{
		JSONArray testData = readObjectArray (WIF);
		for ( int i = 0; i < testData.length (); ++i )
		{
			JSONArray test = testData.getJSONArray (i);
			ECKeyPair kp = ECKeyPair.parseWIF (test.getString (1));
			assertTrue (test.getString (0).equals (kp.getAddress ().toString ()));
			String serialized = ECKeyPair.serializeWIF (kp);
			assertTrue (test.getString (1).equals (serialized));
		}
	}

}
