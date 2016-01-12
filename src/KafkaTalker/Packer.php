<?php
namespace KafkaTalker;

class Packer
{
    public static function pack($format, $dataToPack)
    {
        return pack($format, $dataToPack);
    }

    public static function packSignedInt8($dataToPack)
    {
        return pack('c', $dataToPack);
    }

    public static function packSignedInt16($dataToPack)
    {
        if ($dataToPack < 0) {
            $dataToPack += 0x10000;
        }

        return pack('n', $dataToPack);
    }

    public static function packSignedInt32($dataToPack)
    {
        if ($dataToPack < 0) {
            $dataToPack += 0x100000000;
        }

        return pack('N', $dataToPack);
    }

    public static function packSignedInt64($dataToPack)
    {
        $highMap = 0xffffffff00000000;
        $lowMap = 0x00000000ffffffff;
        $higher = ($dataToPack & $highMap) >> 32;
        $lower = $dataToPack & $lowMap;

        return pack('N2', $higher, $lower);
    }

    public static function packStringSignedInt16($dataToPack)
    {
        if ($dataToPack === null) {
            return self::packSignedInt16(-1);
        }

        return self::packSignedInt16(strlen($dataToPack)) . $dataToPack;
    }

    public static function packStringSignedInt32($dataToPack)
    {
        if ($dataToPack === null) {
            return self::packSignedInt32(-1);
        }

        return self::packSignedInt32(strlen($dataToPack)) . $dataToPack;
    }

    public static function unpack($format, $dataToUnpack)
    {
        $r = unpack($format, $dataToUnpack);

        return array_shift($r);
    }

    public static function unpackSignedInt8($dataToUnpack)
    {
        return self::unpack('c', $dataToUnpack);
    }

    public static function unpackSignedInt16($dataToUnpack)
    {
        $dataToUnpack = self::unpack('n', $dataToUnpack);

        if ($dataToUnpack > 0x7FFF) {
            $dataToUnpack -= 0x10000;
        }

        return $dataToUnpack;
    }

    public static function unpackSignedInt32($dataToUnpack)
    {
        $dataToUnpack = self::unpack('N', $dataToUnpack);

        if ($dataToUnpack > 0x7FFFFFFF) {
            $dataToUnpack -= 0x100000000;
        }

        return $dataToUnpack;
    }

    public static function unpackSignedInt64($dataToUnpack)
    {
        list($higher, $lower) = array_values(unpack('N2', $dataToUnpack));

        return $higher << 32 | $lower;
    }
}
