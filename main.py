from request.ClubsProfile import ClubsProfile
from request.CompetitionClubs import CompetitionClubs


def lambda_handler(event=None, context=None):
    CompetitionClubs().run()
    ClubsProfile().run()


if __name__ == "__main__":
    lambda_handler()
