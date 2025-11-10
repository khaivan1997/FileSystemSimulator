#include "fs/lustre_sim/clients/client_base.hpp"

#include <utility>

namespace sim::fs::lustre_sim {

ClientBase::ClientBase(std::string name, simgrid::s4u::Host* host, ClientParams params)
    : core::ActorBase(std::move(name), host), params_(std::move(params))
{
}

} // namespace sim::fs::lustre_sim
