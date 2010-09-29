package com.ibm.webahead.steer.gizzard.preference

import com.twitter.gizzard.nameserver.{Forwarding, NameServer}
import com.twitter.gizzard.shards.ShardException


class ForwardingManager(nameServer: NameServer[Shard]) extends (Long => Shard) {
  def apply(userId: Long) = nameServer.findCurrentForwarding(0, userId)
}
