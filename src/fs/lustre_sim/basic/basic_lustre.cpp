#include "fs/lustre_sim/basic/basic_lustre.hpp"

#include "core/simulation/actor_base.hpp"
#include "core/report/event_bus.hpp"
#include "core/report/simulation_report.hpp"
#include "fs/lustre_sim/clients/auto_client.hpp"
#include "fs/lustre_sim/constants.hpp"
#include "fs/lustre_sim/components/mds.hpp"
#include "fs/lustre_sim/components/mgs.hpp"
#include "fs/lustre_sim/components/oss.hpp"
#include "fs/lustre_sim/components/ost.hpp"

#include <simgrid/s4u.hpp>

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

std::string make_oss_host_name(std::size_t index)
{
  return std::string(sim::fs::lustre_sim::defaults::OSS_HOST_PREFIX) + std::to_string(index + 1) +
         std::string(sim::fs::lustre_sim::defaults::OSS_HOST_SUFFIX);
}

std::string make_oss_mailbox(std::size_t index)
{
  return std::string(sim::fs::lustre_sim::defaults::OSS_MAILBOX_PREFIX) + std::to_string(index + 1);
}

std::string make_ost_name(std::size_t oss_index, std::size_t ost_index)
{
  return std::string(sim::fs::lustre_sim::defaults::OST_NAME_PREFIX) + std::to_string(oss_index + 1) + "_" +
         std::to_string(ost_index + 1);
}

std::size_t resolve_ost_count(simgrid::s4u::Host* host)
{
  constexpr std::size_t fallback = sim::fs::lustre_sim::defaults::DEFAULT_OSTS_PER_OSS;
  if (host == nullptr)
    return fallback;
  const char* value = host->get_property("ost_count");
  if (value == nullptr)
    return fallback;
  try {
    const auto parsed = static_cast<std::size_t>(std::stoul(value));
    return parsed == 0 ? fallback : parsed;
  } catch (...) {
    return fallback;
  }
}

simgrid::s4u::Host* requireHost(const char* name)
{
  auto* host = simgrid::s4u::Host::by_name(name);
  if (host == nullptr)
    throw std::runtime_error(std::string("host not found: ") + name);
  return host;
}

std::vector<simgrid::s4u::Host*> enumerate_oss_hosts()
{
  std::vector<simgrid::s4u::Host*> hosts;
  for (std::size_t index = 0;; ++index) {
    const auto host_name = make_oss_host_name(index);
    simgrid::s4u::Host* host = nullptr;
    try {
      host = simgrid::s4u::Host::by_name(host_name.c_str());
    } catch (const std::exception&) {
      host = nullptr;
    }
    if (host == nullptr)
      break;
    hosts.push_back(host);
  }
  return hosts;
}

} // namespace

namespace sim::fs::lustre_sim {

int BasicLustre::run(int argc, char* argv[])
{
  simgrid::s4u::Engine engine(&argc, argv);

  const auto exe_dir =
      std::filesystem::canonical(std::filesystem::path(argv[0])).parent_path();
  const auto platform_path = (exe_dir / ".." / "config" / "basic_lustre.xml").lexically_normal();
  engine.load_platform(platform_path.string());
  sim::core::events::publish_annotation("BasicLustre", "PlatformLoaded", platform_path.string());

  auto* host_mgs = requireHost("mgs_host");
  auto* host_mds = requireHost("mds_host");
  auto* host_client = requireHost("client1_host");

  const auto oss_hosts = enumerate_oss_hosts();
  std::vector<std::size_t> ost_counts;
  ost_counts.reserve(oss_hosts.size());
  std::size_t total_osts = 0;
  for (auto* host : oss_hosts) {
    const std::size_t count = resolve_ost_count(host);
    ost_counts.push_back(count);
    total_osts += count;
  }

  sim::core::events::publish_annotation("BasicLustre",
                                        "ClusterLayout",
                                        "OSS hosts=" + std::to_string(oss_hosts.size()) +
                                            ", OSTs=" + std::to_string(total_osts));

  std::vector<std::shared_ptr<sim::core::ActorBase>> actors;
  actors.reserve(3 + oss_hosts.size());

  MgsParams mgs_params{};
  mgs_params.expected_oss_hosts = oss_hosts.size();
  auto mgs = std::make_shared<MgsActor>(std::string(defaults::MGS_MAILBOX), host_mgs, mgs_params);

  MdsParams mds_params{};
  auto mds = std::make_shared<MdsActor>(std::string(defaults::MDS_MAILBOX), host_mds, mds_params);

  OssParams oss_params{};
  oss_params.mds_mailbox = std::string(defaults::MDS_MAILBOX);

  AutoClient::Config client_params{};
  client_params.mgs_mailbox = std::string(defaults::MGS_MAILBOX);
  const std::size_t configured_session_target =
      client_params.session_target > 0 ? client_params.session_target : AutoClient::kDefaultSessionTarget;
  const std::size_t session_target = std::max<std::size_t>(1, configured_session_target);
  const std::size_t stripes_available = total_osts == 0 ? 1 : total_osts;
  const std::size_t per_stripe_goal = (sim::fs::lustre_sim::SimpleOst::DEFAULT_CAPACITY_BYTES / session_target) + 1;
  client_params.desired_file_bytes = per_stripe_goal * stripes_available;
  auto client = std::make_shared<AutoClient>(std::string(defaults::CLIENT_MAILBOX), host_client, client_params);

  sim::core::events::publish_annotation("BasicLustre",
                                        "ClientConfig",
                                        "sessions=" + std::to_string(session_target) +
                                            ", target_bytes=" + std::to_string(client_params.desired_file_bytes));

  actors.emplace_back(mgs);
  actors.emplace_back(mds);
  actors.emplace_back(client);

  engine.add_actor(defaults::MGS_MAILBOX.data(), host_mgs, [mgs]() { mgs->run(); });
  engine.add_actor(defaults::MDS_MAILBOX.data(), host_mds, [mds]() { mds->run(); });
  for (std::size_t oss_index = 0; oss_index < oss_hosts.size(); ++oss_index) {
    const auto mailbox = make_oss_mailbox(oss_index);
    auto* host = oss_hosts[oss_index];

    const std::size_t ost_count = ost_counts.empty() ? resolve_ost_count(host) : ost_counts[oss_index];
    std::vector<std::unique_ptr<sim::storage::OstBase>> oss_disks;
    oss_disks.reserve(ost_count);
    for (std::size_t disk_index = 0; disk_index < ost_count; ++disk_index)
      oss_disks.emplace_back(std::make_unique<SimpleOst>(make_ost_name(oss_index, disk_index)));

    auto oss = std::make_shared<OssActor>(mailbox, host, std::move(oss_disks), oss_params);
    actors.emplace_back(oss);

    engine.add_actor(mailbox, host, [oss]() { oss->run(); });
  }
  engine.add_actor(defaults::CLIENT_MAILBOX.data(), host_client, [client]() { client->run(); });

  sim::core::events::publish_annotation("BasicLustre", "EngineStart", "Actors registered, starting engine loop");
  engine.run();
  sim::core::events::publish_annotation("BasicLustre", "EngineStop", "Engine loop completed");

  const auto report = sim::core::build_simulation_report();
  std::cout << "\n==== Lustre Simulation Report ====\n" << report << std::endl;
  sim::core::events::EventBus::instance().clear();

  return 0;
}

} // namespace sim::fs::lustre_sim
