package org.gbif.metrics.cube.index.taxon.backfill;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class KeyTest {

  @Test
  public void test() {
    UUID u = UUID.randomUUID();
    Key k = new Key(u, 1);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    try {
      k.write(dos);
      dos.flush();
      byte[] bytes = baos.toByteArray();
      Key k2 = new Key();
      k2.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
      assertEquals(k, k2);
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    
  }

}
