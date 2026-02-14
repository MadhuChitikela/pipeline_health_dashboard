
def compute_health_score(success_rate, sla_compliance, quality_score):
    return round(
        (success_rate * 0.5) +
        (sla_compliance * 0.2) +
        (quality_score * 0.3),
        2
    )
