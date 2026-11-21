/**
 * SharedSlotScheduler 离线单元测试（无网络、无服务进程）。
 *
 * 验证两站共享在途名额的分配语义：
 *  A) 只有一个站有请求时，它可以用满全部空闲名额；
 *  B) 两站都在等且名额释放时，严格轮流（round-robin）放行，任一站不被饿死；
 *  C) 任何时刻合计在途不超过共享上限。
 */
import { SharedSlotScheduler } from "../../src/collect/http.js";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
let failures = 0;

function assert(name: string, cond: boolean, detail: string): void {
  if (cond) {
    console.log(`✅ ${name} — ${detail}`);
  } else {
    failures += 1;
    console.error(`❌ ${name} — ${detail}`);
  }
}

async function main(): Promise<void> {
  // ---- A) 单站就绪：用满全部空闲名额 ----
  {
    const cap = 4;
    const s = new SharedSlotScheduler(cap);
    const grants: number[] = [];
    await Promise.all(
      Array.from({ length: cap }, () =>
        s.acquire("A").then((): void => {
          grants.push(s.inflight);
        }),
      ),
    );
    assert(
      "单站就绪时可用满全部空闲名额",
      grants.length === cap && s.inflight === cap && s.stats.highWaterInflight === cap,
      `拿到 ${grants.length}/${cap} 个名额，在途=${s.inflight}，高水位=${s.stats.highWaterInflight}`,
    );
    for (let i = 0; i < cap; i++) s.release("A");
    assert("名额全部释放后在途归零", s.inflight === 0, `inflight=${s.inflight}`);
  }

  // ---- B) 两站都在等：轮流分配 ----
  {
    const cap = 2;
    const s = new SharedSlotScheduler(cap);
    // 先占满两个名额（站 A），制造“名额全满、两站排队”的局面
    await Promise.all([s.acquire("A"), s.acquire("A")]);
    const order: string[] = [];
    const waiters: Promise<void>[] = [];
    // A、B 各排 4 个等待者
    for (let i = 0; i < 4; i++) {
      waiters.push(s.acquire("A").then((): void => { order.push("A"); }));
      waiters.push(s.acquire("B").then((): void => { order.push("B"); }));
    }
    await sleep(30);
    assert("名额满时两站请求均在排队", order.length === 0, `已放行 ${order.length} 个`);

    // 释放 8 次，每次放行一个等待者；记录放行顺序
    for (let i = 0; i < 8; i++) {
      s.release(i % 2 === 0 ? "A" : "B");
      await sleep(5);
    }
    await Promise.all(waiters);
    const aCount = order.filter((x) => x === "A").length;
    const bCount = order.filter((x) => x === "B").length;
    // round-robin：相邻两次放行不应来自同一站
    let alternating = true;
    for (let i = 1; i < order.length; i++) {
      if (order[i] === order[i - 1]) alternating = false;
    }
    assert(
      "两站都在等时轮流分配（无站被饿死且严格交替）",
      aCount === 4 && bCount === 4 && alternating,
      `放行顺序=${order.join("")}，A=${aCount} B=${bCount}`,
    );
    // 释放最初占用的两个名额（已无等待者）；等待者都由 B 的释放触发，
    // 这里两个 A 名额已无人可放，仅回收在途计数。
    s.release("A");
    s.release("A");
    assert("排队清空后释放剩余名额，在途归零", s.inflight === 0, `inflight=${s.inflight}`);
  }

  // ---- C) 高并发压力：合计在途永不超过上限，名额最终全部回收 ----
  {
    const cap = 3;
    const s = new SharedSlotScheduler(cap);
    let peak = 0;
    let violation = false;
    const jobs: Promise<void>[] = [];
    for (let i = 0; i < 40; i++) {
      const station = i % 2 === 0 ? "A" : "B";
      jobs.push(
        (async () => {
          await s.acquire(station);
          if (s.inflight > cap) violation = true;
          peak = Math.max(peak, s.inflight);
          await sleep(2);
          s.release(station);
        })(),
      );
    }
    await Promise.all(jobs);
    assert(
      "高并发下合计在途不超过共享上限且名额全回收",
      !violation && peak <= cap && s.inflight === 0 && peak === cap,
      `峰值=${peak}/${cap}，超限=${violation}，剩余在途=${s.inflight}`,
    );
  }

  if (failures > 0) {
    console.error(`💥 调度器测试 ${failures} 项失败`);
    process.exit(1);
  }
  console.log("🎉 调度器公平性测试全部通过");
}

main().catch((err) => {
  console.error("调度器测试异常：", err);
  process.exit(1);
});
