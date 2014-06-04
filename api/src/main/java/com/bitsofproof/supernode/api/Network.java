package com.bitsofproof.supernode.api;

/**
 * Enumeration of networks. This is used to format addresses to a human readable (BASE58) format with a network specific prefix
 * 
 * <pre>
 * UNKNWON - not defined network
 * PRODUCTION - Bitcoin main network
 * TEST - testnet3 Bitcoin test network
 * </pre>
 */
public enum Network
{
	UNKNOWN, PRODUCTION, TEST
}