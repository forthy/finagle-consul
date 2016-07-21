package com.github.matteobanerjee.finagle.consul.catalog

sealed trait ConsulCheck

case class TtlCheck(TTL: String) extends ConsulCheck
