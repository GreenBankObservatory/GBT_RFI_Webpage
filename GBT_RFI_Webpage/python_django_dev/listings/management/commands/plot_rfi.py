from pathlib import Path
import logging

from django.core.management.base import BaseCommand

import pandas as pd
import dateutil.parser as dp
import matplotlib.pyplot as plt

from listings.models import MasterRfiCatalog
from .mjd import datetime_to_mjd, mjd_to_datetime


LOGGER = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Generate a plot of RFI data. Plots most recent dataset by default."

    def add_arguments(self, parser):
        parser.add_argument(
            "date",
            nargs="?",
            type=dp.parse,
            help="Provide that date on which the RFI data was taken. "
            "Any reasonable format should work",
        )
        parser.add_argument(
            "--show",
            action="store_true",
            help="Show interactive plot",
        )
        parser.add_argument(
            "--output",
            type=Path,
            help="Directory in which to save output files",
        )

    def handle(self, *args, **options):
        if options["date"]:
            date_mjd = datetime_to_mjd(options["date"])
            # Get the nearest MJD (without scanning the whole table)
            mjd = max(
                abs(mjd_)
                for mjd_ in (
                    MasterRfiCatalog.objects.filter(mjd__gte=date_mjd)
                    .order_by("mjd")[:1]
                    .union(
                        MasterRfiCatalog.objects.filter(mjd__lt=date_mjd).order_by(
                            "-mjd"
                        )[:1]
                    )
                    .values_list("mjd", flat=True)
                )
            )
            print(
                "Using nearest MJD value {} ({}) to given date {} ({})".format(
                    mjd, mjd_to_datetime(mjd), date_mjd, options["date"]
                )
            )
        else:
            # Get the most recent mjd value from the DB
            # (Order by mjd column, descending (i.e. newest rows first). Select only the 'mjd' values,
            # and get the first one.)
            mjd = (
                MasterRfiCatalog.objects.order_by("-mjd")
                .values_list("mjd", flat=True)
                .first()
            )
            print("Using latest MJD value {} ({})".format(mjd, mjd_to_datetime(mjd)))

        dt = mjd_to_datetime(mjd)

        print("Querying...")
        data = pd.DataFrame(MasterRfiCatalog.objects.filter(mjd=mjd).values())

        # Write CSV
        csv_filename = options["output"] / "rfi_data_{}.csv".format(
            dt.strftime("%Y-%m-%d_%H-%M-%S")
        )
        print("Wrote {}".format(csv_filename))
        # Write CSV file; don't include index column
        data.to_csv(csv_filename, index=False)

        # Convert from list of (freq, intensity) to two "lists": freqs and intensities
        plot_filename = options["output"] / "rfi_data_{}_plot.png".format(
            dt.strftime("%Y-%m-%d_%H-%M-%S")
        )
        plt.suptitle("RFI Data Plot")
        plt.title(dt)
        plt.xlabel("Frequency (MHZ)")
        plt.ylabel("Intensity (Jy)")
        plt.plot(data["frequency_mhz"], data["intensity_jy"])
        print("Saved plot to {}".format(plot_filename))
        plt.savefig(plot_filename)
