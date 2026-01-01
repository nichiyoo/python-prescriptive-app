from config.settings import config


class Prescriptive:
    """Prescriptive analytics engine for concert recommendations"""

    def __init__(self, df, budget):
        self.df = df.copy()
        self.budget = budget

    def calc_scores(self):
        """
        Calculate prescriptive scores for concert recommendations.

        Multi-criteria scoring algorithm with weighted components:

        1. Cost Score (weight from config, default 40%):
           - Measures cost efficiency relative to most expensive option
           - Formula: 1 - (concert_cost / max_cost)
           - Lower cost = higher score

        2. Budget Remaining Score (weight from config, default 30%):
           - Rewards concerts that leave more budget unused
           - Formula: remaining_budget / max_remaining_budget
           - More remaining budget = higher score

        3. Experience Score (weight from config, default 30%):
           - Values merchandise spending as proxy for experience quality
           - Formula: merchandise_cost / max_merchandise_cost
           - Higher merchandise spending = higher score

        Weights are configurable via .env:
        - WEIGHT_COST (default: 0.4)
        - WEIGHT_REMAINING (default: 0.3)
        - WEIGHT_EXPERIENCE (default: 0.3)

        Final prescriptive_score = (weight_cost * cost) +
                                   (weight_remaining * remaining) +
                                   (weight_experience * experience)

        Returns:
            - None if no concerts within budget
            - (optimal_concert, ranked_dataframe) tuple otherwise
        """
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
            config["weight_cost"] * feasible["score_cost"]
            + config["weight_remaining"] * feasible["score_remaining"]
            + config["weight_experience"] * feasible["score_experience"]
        )

        optimal_idx = feasible["prescriptive_score"].idxmax()
        optimal = feasible.loc[optimal_idx]
        ranked = feasible.sort_values("prescriptive_score", ascending=False)

        return optimal, ranked
