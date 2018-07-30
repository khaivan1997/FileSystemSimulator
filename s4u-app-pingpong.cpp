/* Copyright (c) 2007-2018. The SimGrid Team. All rights reserved.          */

/* This program is free software; you can redistribute it and/or modify it
 * under the terms of the license (GNU LGPL) which comes with this package. */

#include <simgrid/s4u.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_pingpong, "Messages specific for this s4u example");

static void pinger(std::string peer_name)
{
  XBT_INFO("Ping -> %s", peer_name.c_str());
  xbt_assert(simgrid::s4u::Host::by_name_or_null(peer_name) != nullptr, "Unknown host %s. Stopping Now! ",
             peer_name.c_str());

  /* - Do the ping with a 1-Byte task (latency bound) ... */
  double* payload = new double(simgrid::s4u::Engine::get_clock());

  simgrid::s4u::Mailbox::by_name(peer_name)->put(payload, 1);
  /* - ... then wait for the (large) pong */
  double* sender_time =
      static_cast<double*>(simgrid::s4u::Mailbox::by_name(simgrid::s4u::this_actor::get_host()->get_name())->get());

  double communication_time = simgrid::s4u::Engine::get_clock() - *sender_time;
  XBT_INFO("Task received : large communication (bandwidth bound)");
  XBT_INFO("Pong time (bandwidth bound): %.3f", communication_time);
  delete sender_time;
}

static void ponger(std::string peer_name)
{
  XBT_INFO("Pong -> %s", peer_name.c_str());
  xbt_assert(simgrid::s4u::Host::by_name_or_null(peer_name) != nullptr, "Unknown host %s. Stopping Now! ",
             peer_name.c_str());

  /* - Receive the (small) ping first ....*/
  double* sender_time =
      static_cast<double*>(simgrid::s4u::Mailbox::by_name(simgrid::s4u::this_actor::get_host()->get_name())->get());
  double communication_time = simgrid::s4u::Engine::get_clock() - *sender_time;
  XBT_INFO("Task received : small communication (latency bound)");
  XBT_INFO(" Ping time (latency bound) %f", communication_time);
  delete sender_time;

  /*  - ... Then send a 1GB pong back (bandwidth bound) */
  double* payload = new double();
  *payload        = simgrid::s4u::Engine::get_clock();
  XBT_INFO("task_bw->data = %.3f", *payload);

  simgrid::s4u::Mailbox::by_name(peer_name)->put(payload, 1e9);
}

int main(int argc, char* argv[])
{
  simgrid::s4u::Engine e(&argc, argv);

  xbt_assert(argc==2, "Usage: %s platform_file.xml", argv[0]);
  e.load_platform(argv[1]);
   
  simgrid::s4u::Actor::create("pinger", simgrid::s4u::Host::by_name("node-0.acme.org"), pinger, "node-1.acme.org");
  simgrid::s4u::Actor::create("ponger", simgrid::s4u::Host::by_name("node-1.acme.org"), ponger, "node-0.acme.org");

  e.run();

  XBT_INFO("Total simulation time: %.3f", e.get_clock());

  return 0;
}
