/**
  * User:      kaa
  * Timestamp: 02/02/16 16:29
  */
object Rtaus88 {

    var s1 = 3L
    var s2 = 11L
    var s3 = 29L
    var b  = 0L

    @deprecated("uncorrect implementation", "since birth")
    def rand() : Double = {

      b  = ((s1 << 13) ^ s1) >>> 19
      s1 = ((s1 & 4294967294L) << 12) ^ b
      b  = ((s2 << 2) ^ s2) >>> 25
      s2 = ((s2 & 4294967288L) << 4) ^ b
      b  = ((s3 << 3) ^ s3) >>> 11
      s3 = ((s3 & 4294967280L) << 17) ^ b

      (2.3283064365e-10d * (s1 ^ s2 ^ s3)) % 1
    }
}
