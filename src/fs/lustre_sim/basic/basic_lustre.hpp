#pragma once

namespace simgrid::s4u {
class Host;
class Engine;
} // namespace simgrid::s4u

namespace sim::fs::lustre_sim {

class BasicLustre {
public:
  int run(int argc, char* argv[]);
};

} // namespace sim::fs::lustre_sim
