from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtCore import QDate
from data_reader import *
from file_search import *
import sys


class Window(QWidget):
    def __init__(self):
        QWidget.__init__(self)
        layout = QGridLayout()
        self.setLayout(layout)

        #List of receivers
        self.listwidget = QListWidget()
        self.listwidget.setGeometry(90, 350, 101, 20)
        self.listwidget.insertItem(0, "Prime Focus 1 340MHz")
        self.listwidget.insertItem(1, "Prime Focus 1 450 MHz")
        self.listwidget.insertItem(2, "Prime Focus 1 600MHz")
        self.listwidget.insertItem(3, "Prime Focus 1 800MHz")
        self.listwidget.insertItem(4, "Prime Focus 2 1070MHz")
        self.listwidget.insertItem(5, "L Band")
        self.listwidget.insertItem(6, "S Band")
        self.listwidget.insertItem(7, "C Band")
        self.listwidget.insertItem(8, "X Band")
        self.listwidget.insertItem(9, "Ku Band")
        self.listwidget.insertItem(10, "K Band Focal Plane Array")
        self.listwidget.insertItem(11, "Ka Band")
        self.listwidget.insertItem(0, "Q Band")
        self.listwidget.insertItem(1, "W Band")

        #self.listwidget.clicked.connect(self.clicked)
        layout.addWidget(self.listwidget)

        # Start and Stop Date DateEdit widgets
        self.Start_date = QtWidgets.QDateEdit()
        self.Start_date.setGeometry(90, 310, 110, 22)
        layout.addWidget(self.Start_date)
        print(self.Start_date)

        self.stop_date = QtWidgets.QDateEdit(self)
        self.stop_date.setGeometry(230, 310, 110, 22)
        layout.addWidget(self.stop_date)

        # Full or Specific Range Check Box Widgets
        self.Full_Range = QtWidgets.QCheckBox(self)
        self.Full_Range.setGeometry(90, 350, 101, 20)
        self.Full_Range.setText("Full Range")
        layout.addWidget(self.Full_Range)

        self.SpecifiedRange = QtWidgets.QCheckBox(self)
        self.SpecifiedRange.setGeometry(90, 390, 121, 20)
        self.SpecifiedRange.setText("Specified Range")
        layout.addWidget(self.SpecifiedRange)

        # Frequency Range
        self.Start_frequency = QtWidgets.QTextEdit(self)
        self.Start_frequency.setGeometry(90, 430, 104, 51)
        layout.addWidget(self.Start_frequency)

        self.stop_frequency = QtWidgets.QTextEdit(self)
        self.stop_frequency.setGeometry(230, 430, 104, 51)
        layout.addWidget(self.stop_frequency)


        # Push Button to get data
        self.pushButton = QtWidgets.QPushButton(self)
        self.pushButton.setText("Collect data")
        self.pushButton.setGeometry(90, 510, 241, 81)
        layout.addWidget(self.pushButton)
        self.pushButton.clicked.connect(self.clicked)



    def clicked(self):

        def choose_plot(rcvr,graph1,graph2,range1,range2):
            rcvr_list = ["L Band", "S Band", "C Band"]
            rcvr_name = ["Rcvr1_2","Rcvr2_3","Rcvr4_6"]
            for i in range(len(rcvr_list)):
                if rcvr == rcvr_list[i]:
                    file = search(rcvr_name[i])
                    if graph1 == True and graph2 == True:
                        print("Please pick Broad plot or Specified plot, you cannot pick both")

                    elif graph1 == False and graph2 == False:
                        print("Please pick Broad plot or Specified plot")

                    elif graph1 == True:
                        broad_plot(file)

                    elif graph2 == True:
                        print("wokring?")
                        pick_range(file, int(range1), int(range2))

        item = self.listwidget.currentItem()
        Receiver = item.text()


        Begining      = self.Start_date.date().toPyDate()
        end           = self.stop_date.date().toPyDate()

        full          = self.Full_Range.isChecked()
        Specified     = self.SpecifiedRange.isChecked()

        Start         = self.Start_frequency.toPlainText()
        Stop          = self.stop_frequency.toPlainText()

        choose_plot(Receiver,full,Specified,Start,Stop)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    screen = Window()
    screen.show()
    sys.exit(app.exec_())