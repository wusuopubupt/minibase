package org.apache.minibase;

import com.google.common.base.Preconditions;

/**
 * A Bloom filter offers an approximate containment test with one-sided error:
 * if it claims that an element is contained in it, this might be in error,
 * but if it claims that an element is <i>not</i> contained in it, then this is definitely true.
 */
public class BloomFilter {
  private int k;
  private int bitsPerKey;
  private int bitLen;
  private byte[] result;

  public BloomFilter(int k, int bitsPerKey) {
    this.k = k;
    this.bitsPerKey = bitsPerKey;
  }

  public byte[] generate(byte[][] keys) {
    Preconditions.checkNotNull(keys, "keys can not be null!");
    bitLen = keys.length * bitsPerKey;
    bitLen = ((bitLen + 7) / 8) << 3; // align the bitLen.
    bitLen = bitLen < 64 ? 64 : bitLen;
    result = new byte[bitLen >> 3];
    for (int i = 0; i < keys.length; i++) {
      assert keys[i] != null;
      int h = Bytes.hash(keys[i]);
      for (int t = 0; t < k; t++) {
        int idx = (h % bitLen + bitLen) % bitLen;
        result[idx / 8] |= (1 << (idx % 8));
        int delta = (h >> 17) | (h << 15);
        h += delta;
      }
    }
    return result;
  }

  public boolean contains(byte[] key) {
    Preconditions.checkNotNull(result, "result can not be null!");
    int h = Bytes.hash(key);
    for (int t = 0; t < k; t++) {
      int idx = (h % bitLen + bitLen) % bitLen;
      if ((result[idx / 8] & (1 << (idx % 8))) == 0) {
        return false;
      }
      int delta = (h >> 17) | (h << 15);
      h += delta;
    }
    return true;
  }
}
