package com.github.matteobanerjee.finagle.consul

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Filter, Http, Service, SimpleFilter}
import com.twitter.util.Future

import scala.collection.mutable

object ConsulHttpClientFactory {

  type Client = Service[Request, Response]

  private val clients: mutable.Map[String, Client] = mutable.Map()

  def getClient(hosts: String): Client = {
    synchronized {
      val client = clients.getOrElseUpdate(hosts, {
        val filter = new SimpleFilter[Request, Response] {
          override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
            if (!request.headerMap.contains("Host")) {
              request.headerMap.add("Host", "")
            }
            service(request)
          }
        }
        filter andThen Http.newService(hosts)
      })
      client
    }
  }
}
