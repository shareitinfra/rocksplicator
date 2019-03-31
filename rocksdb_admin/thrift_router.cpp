/// Copyright 2016 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author bol (bol@pinterest.com)
//

#include "rocksdb_admin/thrift_router.h"

#include <algorithm>
#include <string>
#include <utility>
#include <vector>
#include "common/jsoncpp/include/json/json.h"
#include "rocksdb/options.h"
#include "rocksdb_admin/utils.h"

DEFINE_bool(always_prefer_local_host, true,
            "Always prefer local host when ordering hosts");
DEFINE_int32(min_client_reconnect_interval_seconds, 5,
             "min reconnect interval in seconds");
DEFINE_int64(client_connect_timeout_millis, 100,
             "Timeout for establishing client connection.");
DEFINE_int32(thrift_router_max_num_hosts_to_consider, -1, "Maximum number of "
             "hosts to consider for each routing request. -1 means unlimited.");
DECLARE_int32(port);
DECLARE_string(rocksdb_dir);
DECLARE_int32(rocksdb_replicator_port);

namespace {

bool parseHost(const std::string& str, admin::detail::Host* host,
               const std::string& segment, const std::string& local_group) {
  std::vector<std::string> tokens;
  folly::split(":", str, tokens);
  if (tokens.size() < 2 || tokens.size() > 3) {
    return false;
  }
  try {
    uint16_t port = atoi(tokens[1].c_str());
    host->addr.setFromIpPort(tokens[0], port);
  } catch (...) {
    return false;
  }
  auto group = (tokens.size() == 3 ? tokens[2] : "");
  host->groups_prefix_lengths[segment] =
    std::distance(group.begin(), std::mismatch(group.begin(), group.end(),
                  local_group.begin(), local_group.end()).first);
  return true;
}

bool parseShard(const std::string& str, admin::detail::Role* role,
                uint32_t* shard) {
  std::vector<std::string> tokens;
  folly::split(":", str, tokens);
  if (tokens.size() < 1 || tokens.size() > 2) {
    return false;
  }

  try {
    *shard = atoi(tokens[0].c_str());
  } catch (...) {
    return false;
  }
  *role = admin::detail::Role::MASTER;
  if (tokens.size() == 2 && tokens[1] == "S") {
    *role = admin::detail::Role::SLAVE;
  }

  return true;
}

std::unique_ptr<rocksdb::DB> GetRocksdb(const std::string& dir,
                                        const rocksdb::Options& options) {
  rocksdb::DB* db;
  auto s = rocksdb::DB::Open(options, dir, &db);
  if (!s.ok()) {
    LOG(ERROR) << "Failed to create db at " << dir << " with error "
               << s.ToString();
    return nullptr;
  }

  return std::unique_ptr<rocksdb::DB>(db);
}

folly::Future<std::unique_ptr<rocksdb::DB>> GetRocksdbFuture(
    const std::string& dir,
    const rocksdb::Options&options) {
  folly::Promise<std::unique_ptr<rocksdb::DB>> promise;
  auto future = promise.getFuture();

  std::thread opener([dir, options,
                      promise = std::move(promise)] () mutable {
      LOG(INFO) << "Start opening " << dir;
      promise.setValue(GetRocksdb(dir, options));
      LOG(INFO) << "Finished opening " << dir;
    });

  opener.detach();

  return future;
}

void LayoutToLocalDetail(std::shared_ptr<const admin::detail::ClusterLayout> cluster_layout,
                         std::map<std::string, std::pair<const folly::SocketAddress, const admin::detail::Role>>* local_detail) {

  folly::SocketAddress local_addr(common::getLocalIPAddress(), FLAGS_port);
  std::map<std::string, std::pair<const folly::SocketAddress, const admin::detail::Role>>& local_detail_ = *local_detail;

  for (const auto& segment : cluster_layout->segments) {
    int shard_id = -1;
    for (const auto& shard : segment.second.shard_to_hosts) {
      ++shard_id;
      bool do_i_own_it = false;
      admin::detail::Role my_role;

      for (const auto& host : shard) {
        if (host.first->addr == local_addr) {
          do_i_own_it = true;
          my_role = host.second;
          break;
        }
      }

      if (!do_i_own_it) {
        continue;
      }

      auto db_name = admin::SegmentToDbName(segment.first.c_str(), shard_id);
      if (my_role == admin::detail::Role::SLAVE) {
        for (const auto& host : shard) {
          if (host.second == admin::detail::Role::MASTER) {
            local_detail_.insert(std::make_pair(std::move(db_name), std::move(std::make_pair(host.first->addr, my_role))));
            break;
          }
        }
      } else {
        local_detail_.insert(std::make_pair(std::move(db_name), std::move(std::make_pair(local_addr, my_role))));
      }
    }
  }
}

}  // namespace

namespace admin {

void AdjustDBBasedOnConfig(std::shared_ptr<const admin::detail::ClusterLayout> cluster_layout_,
                      std::shared_ptr<const admin::detail::ClusterLayout> new_layout,
                      const admin::RocksDBOptionsGeneratorType& rocksdb_options,
                      std::shared_ptr<admin::ApplicationDBManager> db_manager_) {

  std::map<std::string, std::pair<const folly::SocketAddress, const admin::detail::Role>> local_detail_old;
  std::map<std::string, std::pair<const folly::SocketAddress, const admin::detail::Role>> local_detail_new;

  LayoutToLocalDetail(cluster_layout_, &local_detail_old);
  LayoutToLocalDetail(new_layout, &local_detail_new);

  folly::SocketAddress local_addr(common::getLocalIPAddress(), FLAGS_port);

  std::vector<std::function<void(void)>> ops;
  for (const auto& segment : new_layout->segments) {
    int shard_id = -1;
    for (const auto& shard : segment.second.shard_to_hosts) {
      ++shard_id;
      bool do_i_own_it = false;
      admin::detail::Role my_role;

      for (const auto& host : shard) {
        if (host.first->addr == local_addr) {
          do_i_own_it = true;
          my_role = host.second;
          break;
        }
      }

      if (!do_i_own_it) {
        continue;
      }

      auto db_name = admin::SegmentToDbName(segment.first.c_str(), shard_id);

      bool if_modified_db = false;
      if (local_detail_old.find(db_name) != local_detail_old.end()) {
        if (local_detail_old[db_name].first == local_detail_new[db_name].first &&
            local_detail_old[db_name].second == local_detail_new[db_name].second) {
          continue;
        } else {
          if_modified_db = true;
        }
      }

      auto options = rocksdb_options(segment.first);
      auto db_future = GetRocksdbFuture(FLAGS_rocksdb_dir + db_name, options);
      std::unique_ptr<folly::SocketAddress> upstream_addr(nullptr);
      if (my_role == admin::detail::Role::SLAVE) {
        for (const auto& host : shard) {
          if (host.second == admin::detail::Role::MASTER) {
            upstream_addr =
              std::make_unique<folly::SocketAddress>(host.first->addr);
            upstream_addr->setPort(FLAGS_rocksdb_replicator_port);
            break;
          }
        }
      }

      ops.push_back(
        [db_name = std::move(db_name),
         db_future = folly::makeMoveWrapper(std::move(db_future)),
         upstream_addr = folly::makeMoveWrapper(std::move(upstream_addr)),
         my_role, if_modified_db, &db_manager_] () mutable {
          std::string err_msg;
          auto db = (*db_future).get();
          CHECK(db);
          if (if_modified_db) {
              CHECK(db_manager_->removeDB(db_name, &err_msg)) << err_msg;
          }
          if (my_role == admin::detail::Role::MASTER) {
            LOG(ERROR) << "Hosting master " << db_name;
            CHECK(db_manager_->addDB(db_name, std::move(db),
                                    replicator::DBRole::MASTER,
                                    &err_msg)) << err_msg;
            return;
          }

          CHECK(my_role == admin::detail::Role::SLAVE);
          LOG(ERROR) << "Hosting slave " << db_name;
          CHECK(db_manager_->addDB(db_name, std::move(db),
                                  replicator::DBRole::SLAVE,
                                  std::move(*upstream_addr),
                                  &err_msg)) << err_msg;
        });
    }
  }

  for (auto& op : ops) {
    op();
  }

  for (const auto& detail : local_detail_old) {
    if (local_detail_new.find(detail.first) == local_detail_new.end()) {
          std::string err_msg;
          CHECK(db_manager_->removeDB(detail.first, &err_msg)) << err_msg;
    }
  }
}

std::unique_ptr<const detail::ClusterLayout> parseConfig(
    const std::string& content, const std::string& local_group) {
  auto cl = std::make_unique<detail::ClusterLayout>();
  Json::Reader reader;
  Json::Value root;
  if (!reader.parse(content, root) || !root.isObject()) {
    return nullptr;
  }

  static const std::vector<std::string> SHARD_NUM_STRs =
    { "num_leaf_segments", "num_shards" };
  for (const auto& segment : root.getMemberNames()) {
    // for each segment
    const auto& segment_value = root[segment];
    if (!segment_value.isObject()) {
      return nullptr;
    }

    uint32_t shard_number;
    if (segment_value.isMember(SHARD_NUM_STRs[0]) &&
        segment_value[SHARD_NUM_STRs[0]].isInt()) {
      shard_number = segment_value[SHARD_NUM_STRs[0]].asInt();
    } else if (segment_value.isMember(SHARD_NUM_STRs[1]) &&
               segment_value[SHARD_NUM_STRs[1]].isInt()) {
      shard_number = segment_value[SHARD_NUM_STRs[1]].asInt();
    } else {
      LOG(ERROR) << "missing or invalid shard number for " << segment;
      return nullptr;
    }

    cl->segments[segment].shard_to_hosts.resize(shard_number);
    // for each host:port:group
    for (const auto& host_port_group : segment_value.getMemberNames()) {
      if (host_port_group == SHARD_NUM_STRs[0] ||
          host_port_group == SHARD_NUM_STRs[1]) {
        continue;
      }

      detail::Host host;
      if (!parseHost(host_port_group, &host, segment, local_group)) {
        LOG(ERROR) << "Invalid host port group " << host_port_group;
        return nullptr;
      }
      auto host_iter = cl->all_hosts.find(host);
      if (host_iter != cl->all_hosts.end()) {
        host.groups_prefix_lengths.insert(
                host_iter->groups_prefix_lengths.begin(),
                host_iter->groups_prefix_lengths.end());
        cl->all_hosts.erase(host_iter);
      }
      cl->all_hosts.insert(std::move(host));
      const detail::Host* pHost = &*(cl->all_hosts.find(host));
      const auto& shard_list = segment_value[host_port_group];
      // for each shard
      for (Json::ArrayIndex i = 0; i < shard_list.size(); ++i) {
        const auto& shard = shard_list[i];
        if (!shard.isString()) {
          LOG(ERROR) << "Invalid shard list for " << host_port_group;
          return nullptr;
        }

        auto shard_str = shard.asString();
        std::pair<const detail::Host*, detail::Role> p;
        uint32_t shard_id = 0;
        if (!parseShard(shard_str, &p.second, &shard_id) ||
            shard_id >= shard_number) {
          LOG(ERROR) << "Invalid shard " << shard_str;
          return nullptr;
        }
        p.first = pHost;
        cl->segments[segment].shard_to_hosts[shard_id].push_back(p);
      }
    }
  }

  return std::unique_ptr<const detail::ClusterLayout>(std::move(cl));
}

}  // namespace admin
