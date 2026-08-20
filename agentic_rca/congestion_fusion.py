#!/usr/bin/env python3
"""
The RCA's triage confidence formula.

Author: Heven Tafese
"""

import math


LOGISTIC_K = 6.0          
BELIEF_CONGESTION = 0.50  
BELIEF_ONSET = 0.25      
EXONERATE = 0.30


RELIABILITY = {
    "rho_pps":  0.90,   
    "rho_tput": 0.90,   
    "rho_cpu":  0.60,   
    "rho_load": 0.50,   
}


def congestion_ness(ratio: float, k: float = LOGISTIC_K) -> float:
    """Map a utilisation ratio to c in [0,1]. ratio=1.0 (at ceiling) -> 0.5;
    well above -> ~1 (congestion); well below -> ~0 (normal)."""
    return 1.0 / (1.0 + math.exp(-k * (ratio - 1.0)))


def metric_mass(ratio: float, reliability: float) -> dict:
    """Shafer discounted mass over {C},{N},THETA with OR semantics: a saturated
    metric commits strongly to {C}; a healthy metric commits only weakly to {N}
    (EXONERATE), the rest ignorance (THETA)."""
    c = congestion_ness(ratio)
    r = max(0.0, min(1.0, reliability))
    mC = r * c
    mN = r * (1.0 - c) * EXONERATE
    mT = max(0.0, 1.0 - mC - mN)
    return {"C": mC, "N": mN, "T": mT}


def combine(m1: dict, m2: dict):
    """Dempster's rule of combination for {C,N}. Returns (combined mass, K)."""
    c = (m1["C"] * m2["C"] + m1["C"] * m2["T"] + m1["T"] * m2["C"])
    n = (m1["N"] * m2["N"] + m1["N"] * m2["T"] + m1["T"] * m2["N"])
    t = (m1["T"] * m2["T"])
    k = (m1["C"] * m2["N"] + m1["N"] * m2["C"])
    denom = 1.0 - k
    if denom <= 1e-9:
        return ({"C": (m1["C"] + m2["C"]) / 2, "N": (m1["N"] + m2["N"]) / 2,
                 "T": (m1["T"] + m2["T"]) / 2}, k)
    return ({"C": c / denom, "N": n / denom, "T": t / denom}, k)


def fuse(metrics: dict, reliabilities: dict = None) -> dict:
    """Fuse utilisation ratios into a congestion belief. """
    rel = reliabilities or RELIABILITY
    names = [n for n in metrics if n in rel]
    if not names:
        return {"belief_congestion": 0.0, "plausibility_congestion": 1.0,
                "disagreement": 0.0, "band": "baseline", "per_metric": {}}

    per = {n: metric_mass(metrics[n], rel[n]) for n in names}
    combined = None
    conflicts = []
    for n in names:
        if combined is None:
            combined = dict(per[n])
        else:
            combined, k = combine(combined, per[n])
            conflicts.append(k)

    bel_c = combined["C"]
    pl_c = combined["C"] + combined["T"]
    cs = [congestion_ness(metrics[n]) for n in names]
    disagreement = (max(cs) - min(cs)) if len(cs) > 1 else 0.0

    if bel_c >= BELIEF_CONGESTION:
        band = "congestion"
    elif bel_c >= BELIEF_ONSET:
        band = "onset"
    else:
        band = "baseline"

    return {
        "belief_congestion": round(bel_c, 4),
        "plausibility_congestion": round(pl_c, 4),
        "belief_normal": round(combined["N"], 4),
        "uncertainty": round(combined["T"], 4),
        "disagreement": round(disagreement, 4),
        "band": band,
        "per_metric": {n: {"ratio": round(metrics[n], 3),
                           "c": round(congestion_ness(metrics[n]), 3),
                           "reliability": rel[n],
                           "mass_C": round(per[n]["C"], 3),
                           "mass_N": round(per[n]["N"], 3)}
                       for n in names},
    }


if __name__ == "__main__":
    import json
    demo = {"rho_pps": 1.0965, "rho_tput": 0.7291, "rho_cpu": 46.5 / 80,
            "rho_load": 1.145 / 0.8}
    print(json.dumps(fuse(demo), indent=2))
