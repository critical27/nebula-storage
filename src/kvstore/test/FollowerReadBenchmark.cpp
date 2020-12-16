/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/fs/FileUtils.h"
#include "kvstore/NebulaStore.h"
#include <folly/Benchmark.h>

DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace kvstore {

std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers() {
    auto worker =
        apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1, true);
    worker->setNamePrefix("executor");
    worker->start();
    return worker;
}

size_t findStoreIndex(const std::vector<std::unique_ptr<NebulaStore>>& stores,
                      const HostAddr& addr) {
    for (size_t i = 0; i < stores.size(); i++) {
        if (stores[i]->address() == addr) {
            return i;
        }
    }
    LOG(FATAL) << "Should not reach here";
}

void initStore(std::vector<std::unique_ptr<NebulaStore>>& stores, const std::string& rootPath) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    int32_t partCount = 1;
    int32_t replicas = 3;

    auto initNebulaStore = [spaceId, partId] (const std::vector<HostAddr>& peers,
                                              int32_t index,
                                              const std::string& path)
    -> std::unique_ptr<NebulaStore> {
        LOG(INFO) << "Start nebula store on " << peers[index];
        auto ioPool = std::make_shared<folly::IOThreadPoolExecutor>(4);
        auto partMan = std::make_unique<MemPartManager>();
        partMan->addPart(spaceId, partId, peers);

        std::vector<std::string> paths;
        paths.emplace_back(folly::stringPrintf("%s/disk%d", path.c_str(), index));
        KVOptions options;
        options.dataPaths_ = std::move(paths);
        options.partMan_ = std::move(partMan);
        HostAddr local = peers[index];
        return std::make_unique<NebulaStore>(std::move(options),
                                             ioPool,
                                             local,
                                             getWorkers());
    };

    std::string ip("127.0.0.1");
    std::vector<HostAddr> peers;
    for (int32_t i = 0; i < replicas; i++) {
        peers.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }

    for (int i = 0; i < replicas; i++) {
        stores.emplace_back(initNebulaStore(peers, i, rootPath));
        stores.back()->init();
    }

    auto waitLeader = [&stores, replicas, partCount] () {
        while (true) {
            int32_t leaderCount = 0;
            for (int i = 0; i < replicas; i++) {
                std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
                leaderCount += stores[i]->allLeader(leaderIds);
            }
            if (leaderCount == partCount) {
                break;
            }
            usleep(100000);
        }
    };

    LOG(INFO) << "Waiting for all leaders elected!";
    waitLeader();

    bool ready = false;
    do {
        auto addr = stores.front()->partLeader(spaceId, partId);
        CHECK(ok(addr));
        auto leaderIndex = findStoreIndex(stores, value(addr));

        std::vector<KV> data;
        for (auto i = 0; i < 1000; i++) {
            data.emplace_back(folly::stringPrintf("key_%d", i),
                            folly::stringPrintf("val_%d", i));
        }
        folly::Baton<> baton;
        stores[leaderIndex]->asyncMultiPut(
            spaceId, partId, std::move(data), [&baton, &ready] (ResultCode code) {
                if (ResultCode::SUCCEEDED == code) {
                    ready = true;
                }
                baton.post();
            });
        baton.wait();
    } while (!ready);
    sleep(1);
}

void get(const std::vector<std::unique_ptr<NebulaStore>>& stores, int32_t iters) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    auto addr = stores.front()->partLeader(spaceId, partId);
    auto leaderIndex = findStoreIndex(stores, value(addr));
    for (int32_t i = 0; i < iters; i++) {
        std::string value;
        auto code = stores[leaderIndex]->get(spaceId, partId, "key_0", &value);
        CHECK_EQ(ResultCode::SUCCEEDED, code);
        CHECK_EQ("val_0", value);
    }
}

void asyncGet(const std::vector<std::unique_ptr<NebulaStore>>& stores, int32_t iters) {
    GraphSpaceID spaceId = 0;
    PartitionID partId = 0;
    auto addr = stores.front()->partLeader(spaceId, partId);
    auto followerIndex = (findStoreIndex(stores, value(addr)) + 1) % 3;
    for (int32_t i = 0; i < iters; i++) {
        std::string value;
        auto code = stores[followerIndex]->asyncGet(spaceId, partId, "key_0", &value).get();
        CHECK_EQ(ResultCode::SUCCEEDED, code);
        CHECK_EQ("val_0", value);
    }
}

BENCHMARK(LeaderRead, iters) {
    fs::TempDir rootPath("/tmp/nebula_store_bm.XXXXXX");
    std::vector<std::unique_ptr<NebulaStore>> stores;
    BENCHMARK_SUSPEND {
        initStore(stores, rootPath.path());
    };
    get(stores, iters);
}

BENCHMARK(FollowerRead, iters) {
    fs::TempDir rootPath("/tmp/nebula_store_bm.XXXXXX");
    std::vector<std::unique_ptr<NebulaStore>> stores;
    BENCHMARK_SUSPEND {
        initStore(stores, rootPath.path());
    };
    asyncGet(stores, iters);
}

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
    FLAGS_minloglevel = 3;
    FLAGS_raft_heartbeat_interval_secs = 1;
    folly::init(&argc, &argv, true);
    folly::runBenchmarks();
    return 0;
}
