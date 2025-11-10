#include "fs/lustre_sim/basic/basic_lustre.hpp"

int main(int argc, char* argv[])
{
  sim::fs::lustre_sim::BasicLustre simulation;
  return simulation.run(argc, argv);
}
