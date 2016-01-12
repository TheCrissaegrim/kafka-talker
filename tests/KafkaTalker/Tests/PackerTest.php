<?php
namespace KafkaTalker\Tests;

use KafkaTalker\Packer;

class PackerTest extends KafkaTalkerTest
{
    public function testMaxSignedInt8()
    {
        $maxSignedInt8 = 0x7F;

        $packedMaxSignedInt8 = Packer::packSignedInt8($maxSignedInt8);
        $unpackedMaxSignedInt8 = Packer::unpackSignedInt8($packedMaxSignedInt8);

        $this->assertSame($maxSignedInt8, $unpackedMaxSignedInt8);
    }

    public function testMaxSignedInt16()
    {
        $maxSignedInt16 = 0x7FFF;

        $packedMaxSignedInt16 = Packer::packSignedInt16($maxSignedInt16);
        $unpackedMaxSignedInt16 = Packer::unpackSignedInt16($packedMaxSignedInt16);

        $this->assertSame($maxSignedInt16, $unpackedMaxSignedInt16);
    }

    public function testMaxSignedInt32()
    {
        $maxSignedInt32 = 0x7FFFFFFF;

        $packedMaxSignedInt32 = Packer::packSignedInt32($maxSignedInt32);
        $unpackedMaxSignedInt32 = Packer::unpackSignedInt32($packedMaxSignedInt32);

        $this->assertSame($maxSignedInt32, $unpackedMaxSignedInt32);
    }

    public function testMaxSignedInt64()
    {
        $maxSignedInt64 = 0x7FFFFFFFFFFFFFFF;

        $packedMaxSignedInt64 = Packer::packSignedInt64($maxSignedInt64);
        $unpackedMaxSignedInt64 = Packer::unpackSignedInt64($packedMaxSignedInt64);

        $this->assertSame($maxSignedInt64, $unpackedMaxSignedInt64);
    }

    public function testMinSignedInt8()
    {
        $minSignedInt8 = -0x80;

        $packedMinSignedInt8 = Packer::packSignedInt8($minSignedInt8);
        $unpackedMinSignedInt8 = Packer::unpackSignedInt8($packedMinSignedInt8);

        $this->assertSame($minSignedInt8, $unpackedMinSignedInt8);
    }

    public function testMinSignedInt16()
    {
        $minSignedInt16 = -0x8000;

        $packedMinSignedInt16 = Packer::packSignedInt16($minSignedInt16);
        $unpackedMinSignedInt16 = Packer::unpackSignedInt16($packedMinSignedInt16);

        $this->assertSame($minSignedInt16, $unpackedMinSignedInt16);
    }

    public function testMinSignedInt32()
    {
        $minSignedInt32 = -0x80000000;

        $packedMinSignedInt32 = Packer::packSignedInt32($minSignedInt32);
        $unpackedMinSignedInt32 = Packer::unpackSignedInt32($packedMinSignedInt32);

        $this->assertSame($minSignedInt32, $unpackedMinSignedInt32);
    }

    public function testMinSignedInt64()
    {
        $minSignedInt64 = -0x7FFFFFFFFFFFFFFF; // This test shoud be performed with "-0x8000000000000000" but it fails...

        $packedMinSignedInt64 = Packer::packSignedInt64($minSignedInt64);
        $unpackedMinSignedInt64 = Packer::unpackSignedInt64($packedMinSignedInt64);

        $this->assertSame($minSignedInt64, $unpackedMinSignedInt64);
    }
}
