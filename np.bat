git status
git log --branches --not --remotes
git config user.name
git config user.email
git branch
rem if you have something already running at port 80, the default configuration of the app might not work (especially for the users authentication piece with Dreamfactory)
rem For example, a process shown something like DESKTOP-RI8S27P:0 means you have IIS 10.0 Express running and that has to be uninstalled first before you install Dreamfactory (DF)!
rem Otherwise DF will install itself via port 81 or higher and this app would not work out of the box!
:netstat -an | find ":80"
:netstat -ab | find ":80"