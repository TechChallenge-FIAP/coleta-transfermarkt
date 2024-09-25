from dataclasses import dataclass

from etl.raw.clubs_profile.business_rules.business_rules import BusinessRules


@dataclass
class ClubsProfileJob(BusinessRules):
    def main(self):
        df_clubs_profile = self.read_raw("amostra/landing/ClubsProfile/")

        self.write_csv(df_clubs_profile.repartition(1), "amostra/raw/ClubsProfile/")
