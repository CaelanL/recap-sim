"""CLI for the Confluence Recap SIMULATION.

  python3 -m recap_sim run --scenario baseline|bug [--seed N]
  python3 -m recap_sim replay --scenario bug [--seed N]   (runs twice, compares digests)
  python3 -m recap_sim experiment experiments/<file>.json
  python3 -m recap_sim serve [--port 8710]                (animated UI)
"""
import argparse, json, sys

from .sim import Simulation, SCENARIOS, check_invariants, run_experiment


def _summary_text(s):
    lines = [
        "Confluence Recap pipeline - SIMULATION (synthetic data, not real AWS)",
        "  seed=%s  sim_minutes=%s  events_fired=%s" % (
            s["seed"], s["sim_minutes"], s["events_fired"]),
        "  recaps_emitted=%s  adf_reports_written=%s" % (
            s["recaps_emitted"], s["adf_reports_written"]),
        "  duplicate_report_rows=%s %s" % (
            s["duplicate_report_rows"],
            s["duplicate_examples"] if s["duplicate_report_rows"] else ""),
        "  cc_notifications=%s  duplicate_notifications=%s" % (
            s["cc_notifications"], s["duplicate_notifications"]),
        "  idempotency processed_records: %s" % s["idempotency"]["processed_records"],
        "  queues:",
    ]
    for name, q in s["queues"].items():
        lines.append("    %-24s depth=%-4s in_flight=%-3s to_dlq=%s redriven_in=%s"
                     % (name, q["depth"], q["in_flight"], q["to_dlq"],
                        q["redriven_in"]))
    lines.append("  digest=%s" % s["digest"])
    return "\n".join(lines)


def cmd_run(args):
    cfg = SCENARIOS[args.scenario](seed=args.seed)
    sim = Simulation(cfg)
    sim.run()
    print(_summary_text(sim.summary()))
    print("\nInvariants:")
    for r in check_invariants(sim):
        print("  [%s] %s  %s" % ("PASS" if r["passed"] else "FAIL",
                                 r["invariant"], r["evidence"]))
    return 0


def cmd_replay(args):
    cfg = SCENARIOS[args.scenario](seed=args.seed)
    digests = []
    for i in (1, 2):
        sim = Simulation(cfg)
        sim.run()
        s = sim.summary()
        digests.append(s["digest"])
        print("--- replay run %d ---" % i)
        print(_summary_text(s))
    same = digests[0] == digests[1]
    print("\nDeterminism: digest run1 == digest run2 -> %s (%s)"
          % (same, digests[0][:16]))
    print("The injected failure reproduces exactly on seed %s." % args.seed
          if same else "MISMATCH: runs diverged!")
    return 0 if same else 1


def cmd_experiment(args):
    spec = json.load(open(args.path))
    out = run_experiment(spec)
    print(json.dumps(out, indent=2, default=str))
    return 0 if out["all_passed"] else 2


def _serve(args):
    from .server import serve
    serve(port=args.port)
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(prog="recap_sim")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run")
    p.add_argument("--scenario", default="baseline", choices=sorted(SCENARIOS))
    p.add_argument("--seed", type=int, default=None)
    p.set_defaults(fn=cmd_run)

    p = sub.add_parser("replay")
    p.add_argument("--scenario", default="bug", choices=sorted(SCENARIOS))
    p.add_argument("--seed", type=int, default=1337)
    p.set_defaults(fn=cmd_replay)

    p = sub.add_parser("experiment")
    p.add_argument("path")
    p.set_defaults(fn=cmd_experiment)

    p = sub.add_parser("serve")
    p.add_argument("--port", type=int, default=8710)
    p.set_defaults(fn=_serve)

    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
