from pathlib import Path
import logging

from django.core.management.base import BaseCommand
from django.db.models import Max

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
            "-d",
            "--date",
            type=dp.parse,
            help="Provide a date close to when the RFI data was taken. "
            "Any reasonable format should work, e.g. 'YYYY/MM/DD HH:mm:ss'",
        )
        parser.add_argument(
            "-s",
            "--start",
            type=dp.parse,
            help="Provide a start date"
            "Any reasonable format should work, e.g. 'YYYY/MM/DD HH:mm:ss'",
        )
        parser.add_argument(
            "-e",
            "--end",
            type=dp.parse,
            help="Provide an end date"
            "Any reasonable format should work, e.g. 'YYYY/MM/DD HH:mm:ss'",
        )
        parser.add_argument(
            "-r",
            "--receivers",
            choices=sorted(
                MasterRfiCatalog.objects.values_list("frontend", flat=True).distinct()
            ),
            nargs="+",
            help="Select data only from given receiver(s)",
        )
        parser.add_argument(
            "-F",
            "--frequency",
            type=float,
            help="Middle frequency",
        )
        parser.add_argument(
            "-b",
            "--buffer",
            type=float,
            help="Frequency filter window size",
            default=500.0,
        )
        parser.add_argument(
            "--show",
            action="store_true",
            help="Show interactive plot",
        )
        parser.add_argument(
            "--output",
            type=Path,
            default=Path("."),
            help="Directory in which to save output files",
        )

    def handle(self, *args, **options):
        rows = MasterRfiCatalog.objects.all()
        if options["receivers"]:
            print("Selecting scans from receivers: {}".format(options["receivers"]))
            rows = rows.filter(frontend__in=options["receivers"])

        if options["frequency"]:
            lower_bound = options["frequency"] - options["buffer"] / 2
            upper_bound = options["frequency"] + options["buffer"] / 2
            print(
                "Selecting scans between frequencies {} MHz - {} MHz".format(
                    lower_bound, upper_bound
                )
            )
            rows = rows.filter(
                frequency_mhz__gte=lower_bound, frequency_mhz__lte=upper_bound
            )

        if options["date"]:
            date_mjd = datetime_to_mjd(options["date"])
            # Get the nearest MJD (without scanning the whole table)
            mjd = max(
                abs(mjd_)
                for mjd_ in (
                    rows.filter(mjd__gte=date_mjd)
                    .order_by("mjd")[:1]
                    .union(rows.filter(mjd__lt=date_mjd).order_by("-mjd")[:1])
                    .values_list("mjd", flat=True)
                )
            )
            print(
                "Using nearest MJD value {} ({}) to given date {} ({})".format(
                    mjd, mjd_to_datetime(mjd), date_mjd, options["date"]
                )
            )
            rows = rows.filter(mjd=mjd)
            start_mjd = mjd
            end_mjd = mjd
        elif options["start"] or options["end"]:
            start_mjd = datetime_to_mjd(options["start"])
            end_mjd = datetime_to_mjd(options["end"])
            if options["start"]:
                print("Selecting scans starting after {}".format(start_mjd))
                rows = rows.filter(mjd__gte=start_mjd)
            if options["end"]:
                print("Selecting scans starting before {}".format(end_mjd))
                rows = rows.filter(mjd__lte=end_mjd)
        else:
            print("calc max")
            mjd = rows.aggregate(Max("mjd"))["mjd__max"]
            print("Using latest MJD value {} ({})".format(mjd, mjd_to_datetime(mjd)))
            rows = rows.filter(mjd=mjd)
            start_mjd = mjd
            end_mjd = mjd

        # Filter by MJD
        print("Querying...")
        data = pd.DataFrame(rows.values())
        data.insert(
            loc=list(data.columns).index("mjd"),
            column="dt",
            value=[mjd_to_datetime(_mjd) for _mjd in data["mjd"]],
        )
        if data.empty:
            print("No results found :(")
            return

        print("Creating summary table...")
        summary = data[
            [
                "dt",
                "mjd",
                "frontend",
                "frequency_mhz",
                "intensity_jy",
            ]
        ].groupby(["dt", "mjd", "frontend"])
        summary = summary.min()
        print("Found {} unique RFI sessions:".format(len(summary)))
        print(summary.to_string())
        print("-" * 20)
        start_dt = mjd_to_datetime(start_mjd)
        end_dt = mjd_to_datetime(end_mjd)
        date_range_str = "{}-{}".format(
            start_dt.strftime("%Y_%m_%d_%H_%M_%S"), end_dt.strftime("%Y_%m_%d_%H_%M_%S")
        )
        # Write CSV
        receivers_stub = "-" + "-".join(options["receivers"]) if options["receivers"] else ""
        filename_stub = "{date_range_str}{receivers}".format(
            date_range_str=date_range_str, receivers=receivers_stub
        )
        csv_path = options["output"] / "rfi_data-{}.csv".format(filename_stub)
        print("Saved CSV to {}".format(csv_path))
        # Write CSV file; don't include index column
        data.to_csv(csv_path, index=False)

        # Convert from list of (freq, intensity) to two "lists": freqs and intensities
        plot_filename = options["output"] / "rfi_data_plot-{}.png".format(filename_stub)
        plt.suptitle("RFI Data Plot")
        plt.title(date_range_str)
        plt.xlabel("Frequency (MHZ)")
        plt.ylabel("Intensity (Jy)")
        plt.plot(data["frequency_mhz"], data["intensity_jy"])
        print("Saved plot to {}".format(plot_filename))
        plt.savefig(plot_filename)
