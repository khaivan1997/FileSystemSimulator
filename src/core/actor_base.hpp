#pragma once

#include "core/message.hpp"

#include <simgrid/s4u.hpp>

#include <string>
#include <vector>

namespace sim::core {

class ActorBase {
public:
  ActorBase(std::string name, simgrid::s4u::Host* host);
  virtual ~ActorBase() = default;

  const std::string& name() const { return name_; }
  simgrid::s4u::Host* host() const { return host_; }

  virtual void run() = 0;

protected:
  using CommPtr = simgrid::s4u::CommPtr;

  void send(const std::string& dst, const Message& msg);
  CommPtr sendAsync(const std::string& dst, const Message& msg);

  Message* receiveBlocking();
  CommPtr receiveAsync();
  Message* tryReceive(CommPtr& comm);
  void reapPendingSends();

  std::string name_;
  simgrid::s4u::Host* host_{nullptr};

private:
  simgrid::s4u::Mailbox* mailbox_{nullptr};
  Message* pending_async_msg_{nullptr};
  std::vector<CommPtr> pending_async_sends_;
};

} // namespace sim::core
