# src/exporter.py

import pandas as pd

class Exporter:
    @staticmethod
    def export(tracking, summary, out_path):
        perf = []
        comp = []
        changes = []
        # For cumulative return, we need the first index value
        if not tracking:
            raise ValueError("Tracking is empty!")
        first_index_value = tracking[0]["Index Value"]
        for entry in tracking:
            cumulative_return = (entry["Index Value"] / first_index_value) - 1
            perf.append({
                "Date": entry["Date"],
                "Index Value": entry["Index Value"],
                "Daily Return": entry["Daily Return"],
                "Cumulative Return": cumulative_return
            })
            comp.append({
                "Date": entry["Date"],
                "Constituents": ",".join(entry["Constituents"])
            })
            # if entry["Added"] or entry["Removed"]:
            changes.append({
                    "Date": entry["Date"],
                    "Added": ",".join(entry["Added"]),
                    "Removed": ",".join(entry["Removed"])
                })
        summary_df = pd.DataFrame([summary])

        with pd.ExcelWriter(out_path) as writer:
            pd.DataFrame(perf).to_excel(writer, sheet_name="index_performance", index=False)
            pd.DataFrame(comp).to_excel(writer, sheet_name="daily_composition", index=False)
            pd.DataFrame(changes).to_excel(writer, sheet_name="composition_changes", index=False)
            summary_df.to_excel(writer, sheet_name="summary_metrics", index=False)
