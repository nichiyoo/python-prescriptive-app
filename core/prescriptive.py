class Prescriptive:
    """Prescriptive analytics engine for concert recommendations"""

    def __init__(self, df, budget):
        self.df = df.copy()
        self.budget = budget

    def calc_scores(self):
        """Calculate prescriptive scores and return optimal recommendation"""
        feasible = self.df[self.df["total_pengeluaran"] <= self.budget].copy()

        if feasible.empty:
            return None

        feasible["score_cost"] = 1 - (
            feasible["total_pengeluaran"] / feasible["total_pengeluaran"].max()
        )

        feasible["sisa_budget"] = self.budget - feasible["total_pengeluaran"]
        feasible["score_remaining"] = (
            feasible["sisa_budget"] / feasible["sisa_budget"].max()
        )

        feasible["score_experience"] = (
            feasible["merchandise"] / feasible["merchandise"].max()
        )

        feasible["prescriptive_score"] = (
            0.4 * feasible["score_cost"]
            + 0.3 * feasible["score_remaining"]
            + 0.3 * feasible["score_experience"]
        )

        optimal_idx = feasible["prescriptive_score"].idxmax()
        optimal = feasible.loc[optimal_idx]
        ranked = feasible.sort_values("prescriptive_score", ascending=False)

        return optimal, ranked
