from datetime import datetime
import logging

from django.core.management.base import BaseCommand

import pandas as pd
from tqdm import tqdm

from listings.models import MasterRfiCatalog
from .mjd import datetime_to_mjd, mjd_to_datetime


LOGGER = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Generate a summary of RFI data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--start",
            help="Start date (MM/DD/YYYY)",
        )
        parser.add_argument(
            "--end",
            help="End date (MM/DD/YYYY)",
        )

    def handle(self, *args, **options):
        rfi_rows = MasterRfiCatalog.objects.all()
        filter_info = []
        if options["start"]:
            start_dt = datetime.strptime(options["start"], "%Y/%m/%d")
            start_mjd = datetime_to_mjd(start_dt)
            rfi_rows = rfi_rows.filter(mjd__gt=start_mjd)
            filter_info.append(" after {}".format(start_dt))
        if options["end"]:
            end_dt = datetime.strptime(options["end"], "%Y/%m/%d")
            end_mjd = datetime_to_mjd(end_dt)
            rfi_rows = rfi_rows.filter(mjd__lt=end_mjd)
            filter_info.append(" before {}".format(end_dt))
        unique_mjds = rfi_rows.values_list("mjd", flat=True).distinct()

        rows = []
        print("Found {} observations{}".format(unique_mjds.count(), " and".join(filter_info)))
        for mjd in tqdm(unique_mjds):
            # Get freq/intensity info from all rows that match this mjd value
            frontend, projid, backend, mjd = (
                MasterRfiCatalog.objects.filter(mjd=mjd)
                .values_list("frontend", "projid", "backend", "mjd")
                .first()
            )
            # print(frontend, projid, backend, mjd )
            dt = mjd_to_datetime(mjd)
            rows.append([dt, projid, frontend, backend])

        df = pd.DataFrame(
            rows, columns=("Date Observed", "Project", "Frontend", "Backend")
        )
        print(df)
