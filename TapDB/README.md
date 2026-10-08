# TapDB

Open-source repository for *"Understanding and Optimizing Database Pushdown on Disaggregated Storage" (ASPLOS'26)*.

TapDB is a new **Pushdown Database** designed for emerging storage disaggregation architectures. TapDB makes informed database operator pushdown decisions based on the actual communication-computation trade-off at **runtime**, mitigates the performance interference, and sparingly schedules co-located operators to maximize performance. TapDB is built on top of [FlexPushdownDB-VLDB](https://github.com/cloud-olap/FlexPushdownDB/tree/vldbj_24) and [FlexPushdownDB-VLDBJ](https://github.com/cloud-olap/FlexPushdownDB/tree/vldb_21).

## System Requirements

- **LLVM**: Version 10 or later
- **Operating System**: Ubuntu 20.04 or later
- **Additional Requirements**: See `tools/project/bin/ubuntu-prerequisites.sh` for a complete list of prerequisites

**Note:** If you are using Ubuntu 16.04, we recommend compiling TapDB on Ubuntu 20.04 and then copying the dynamic libraries to your Ubuntu 16.04 system.

## Getting Started

### Clone the Repository

```bash
git clone https://github.com/netlab-wisconsin/TapDB.git
cd TapDB
```

### On Each Node

1. Install prerequisites:
   ```bash
   sudo ./tools/project/bin/ubuntu-prerequisites.sh
   ```

### On Master Node

Ensure the master node has SSH access to other nodes.

1. **Generate Data:**
   - For TPC-H: `./resources/script/tpch-data-gen.sh <scale-factor>`
   - For SSB: 
     ```bash
     ./resources/script/ssb-data-gen.sh <scale-factor>
     ```
     Then enter `ssb-dbgen` directory and make the following modifications:
     - Edit function `tbl_open` in `bm_utils.c`: append `0777` after `O_CREAT` as the third parameter of `open`.
     - Edit definition `MAXAGG_LEN` in `shared.h`: change `10` to `20`.
     - Run SSB data generation again.

2. Edit `./resources/config/cluster_ips` (make sure `127.0.0.1` is set for the master node).

3. Edit `RESOURCE_PATH` in `./fpdb-calcite/java/main/resources/config/exec.conf`.

4. Edit `deploy_dir_name` and `username` in `./resources/script/util.sh`.

5. Set up the system:
   ```bash
   ./resources/script/setup.sh
   ```
   If all required dependencies are already installed, you can also set up the system by:
   ```bash
   ./resources/script/build.sh -d
   ./resources/script/deploy.sh
   ```

### Run

1. `cd <deploy_dir_name>`
2. Start the system:
   ```bash
   ./resources/script/start.sh
   ```
3. `cd fpdb-main`
4. Run tests:
   - For TPC-H: `./fpdb-main-test -ts=tpch-sf1-distributed`
   - For SSB: `./fpdb-main-test -ts=ssb-sf1-single_node-no-parallels`
5. When finished, stop the system:
   ```bash
   ./resources/script/stop.sh
   ```

## Restart Remote Server

There is a file called `restart.sh` on the remote server:

```bash
ps -ef | awk '/fpdb/ && !/awk/{print $2}' | xargs kill -9
sleep 60s
screen -S test -p 0 -X stuff "./fpdb-main-server > result ^M"
```

- Line 1: kills the old server process
- Line 2: waits to ensure the kill operation is successful
- Line 3: restarts `fpdb-main-server`

**Note:** You must have installed `screen` and created a screen session called `test`. The screen session called `test` must already be in the directory where `fpdb-main-server` is located.

To restart remote `fpdb-main-server`, configure SSH key authentication and run the remote script:

```bash
ssh user@storage-node "/Work/restart.sh"
```

## Batch Testing

1. Make sure `calcite` is running (`./resources/script/start.sh`).
2. On the server node:
   - Create a new screen: `screen -S server`
   - Navigate to the directory: `cd <deploy_dir>/fpdb-main`
   - Detach from screen: press `Ctrl+A` then `D`
3. On the client node:
   - Create a new screen: `screen -S batch`
   - Run the batch test script:
     ```bash
     cd <deploy_dir>/fpdb-main && bash <deploy_dir>/script/test_run_new.sh
     ```

## Compiling TapDB on Different Ports

### Modify Calcite Port

Edit `SERVER_PORT` in the following files:
- `fpdb-calcite/java/main/resources/config/exec.conf`
- `fpdb-calcite/java/main/resources/config/exec.conf.ec2`
- `resources/config/calcite.conf`

### Modify FPDB-Main-Server Port

Edit `CAF_SERVER_PORT` in `resources/config/exec.conf` (line 9). **Note:** The temporary path also uses this port, so this must be changed.

### Modify FPDB-Build Path

Edit `deploy_dir_name` in `resources/script/util.sh` (line 4) to the corresponding `FPDB-build-${}` path (i.e., the current FPDB path).

### Modify RDMA Port

Edit `port` in `resources/config/port.conf`.


## Run on GEN4-SA Nodes

TapDB can run on Stingray PS1100R storage nodes. Each node encloses a PS1100R SmartNIC, a PCIe carrier board, 4 NVMe SSDs, and a power supply. The carrier board holds both a SmartNIC and NVMe drives, containing an on-board PCIe switch that connects various components.

To use TapDB with GEN4-SA nodes, you can use our provided [CAF-on-SmartNIC](https://github.com/thunderZH963/CAF-on-SmartNIC) to replace `fpdb-caf`.
