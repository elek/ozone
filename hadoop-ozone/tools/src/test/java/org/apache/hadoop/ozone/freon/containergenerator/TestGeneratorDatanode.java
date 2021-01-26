package org.apache.hadoop.ozone.freon.containergenerator;

import org.junit.Assert;
import static org.junit.Assert.*;
import org.junit.Test;

public class TestGeneratorDatanode {

  @Test
  public void indexForOnePipeline() {
    final GeneratorDatanode generatorDatanode = new GeneratorDatanode();
    generatorDatanode.setThreadNo(50);
    generatorDatanode.setDatanodeIndex(1);
    generatorDatanode.setDatanodes(3);
    generatorDatanode.setNumberOfPipelines(3);
    generatorDatanode.setContainerIdOffset(1);

    for (int i = 1; i <= 3; i++) {
      generatorDatanode.setDatanodeIndex(i);
      generatorDatanode.initIndexParameters();
      Assert.assertEquals(1L, generatorDatanode.getContainerIdForIndex(0L));
      Assert.assertEquals(2L, generatorDatanode.getContainerIdForIndex(1L));
      Assert.assertEquals(3L, generatorDatanode.getContainerIdForIndex(2L));
      Assert.assertEquals(4L, generatorDatanode.getContainerIdForIndex(3L));
      Assert.assertEquals(5L, generatorDatanode.getContainerIdForIndex(4L));
      Assert.assertEquals(6L, generatorDatanode.getContainerIdForIndex(5L));
    }
  }

  @Test
  public void indexForNineNode() {
    final GeneratorDatanode generatorDatanode = new GeneratorDatanode();
    generatorDatanode.setThreadNo(50);
    generatorDatanode.setDatanodeIndex(1);
    generatorDatanode.setDatanodes(3);
    generatorDatanode.setNumberOfPipelines(3);
    generatorDatanode.setContainerIdOffset(1);

    generatorDatanode.setDatanodeIndex(1);
    Assert.assertEquals(1L, generatorDatanode.getContainerIdForIndex(1L));
    Assert.assertEquals(2L, generatorDatanode.getContainerIdForIndex(2L));
    Assert.assertEquals(3L, generatorDatanode.getContainerIdForIndex(3L));
    Assert.assertEquals(4L, generatorDatanode.getContainerIdForIndex(4L));
    Assert.assertEquals(5L, generatorDatanode.getContainerIdForIndex(5L));

    generatorDatanode.setDatanodeIndex(2);
    Assert.assertEquals(1L, generatorDatanode.getContainerIdForIndex(1L));
    Assert.assertEquals(2L, generatorDatanode.getContainerIdForIndex(2L));
    Assert.assertEquals(3L, generatorDatanode.getContainerIdForIndex(3L));
    Assert.assertEquals(4L, generatorDatanode.getContainerIdForIndex(4L));
    Assert.assertEquals(5L, generatorDatanode.getContainerIdForIndex(5L));

    generatorDatanode.setDatanodeIndex(3);
    Assert.assertEquals(1L, generatorDatanode.getContainerIdForIndex(1L));
    Assert.assertEquals(2L, generatorDatanode.getContainerIdForIndex(2L));
    Assert.assertEquals(3L, generatorDatanode.getContainerIdForIndex(3L));
    Assert.assertEquals(4L, generatorDatanode.getContainerIdForIndex(4L));
    Assert.assertEquals(5L, generatorDatanode.getContainerIdForIndex(5L));

  }

}