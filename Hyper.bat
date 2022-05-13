call "C:\Program Files\Tableau\Tableau Prep Builder 2021.1\scripts\tableau-prep-cli.bat" -c"D:\work\Daily\Key.json" -t"D:\work\Daily\Daily.tfl"

call "C:\Program Files\Tableau\Tableau Prep Builder 2021.1\scripts\tableau-prep-cli.bat" -c"D:\work\Daily\Key.json" -t"D:\work\Daily\ThirdParty.tfl"

call "C:\Program Files\Tableau\Tableau Prep Builder 2021.1\scripts\tableau-prep-cli.bat" -c"D:\work\Daily\Key.json" -t"D:\work\Daily\log.tfl"

call "C:\Program Files\Tableau\Tableau Prep Builder 2021.1\scripts\tableau-prep-cli.bat" -c"D:\work\Daily\Key.json" -t"D:\work\Daily\Operation.tfl"

cd /d "C:\Program Files\Tableau\Tableau Server\2021.1\extras\Command Line Utility"

tabcmd login -s ***** -u ***** -p *****

tabcmd publish "D:\work\Daily\Hyper\Daily.hyper" -r "DataSource" -n "Daily" -o

tabcmd publish "D:\work\Daily\Hyper\ThirdParty2.hyper" -r "DataSource" -n "ThirdParty2" -o

tabcmd publish "D:\work\Daily\Hyper\log.hyper" -r "DataSource" -n "log" -o

tabcmd publish "D:\work\Daily\Hyper\Operation.hyper" -r "DataSource" -n "Operation" -o

del "D:\work\Daily\Hyper\*.hyper"