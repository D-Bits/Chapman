from subprocess import run

deploy = run(['pyinstaller', 'main.py', '-F', '-n', 'PyETL'])

if deploy.returncode == 0:
    input('Deployment successful. Press enter to exit.')
else:
    input('WARNING: Deployment failed. Press enter to exit.')