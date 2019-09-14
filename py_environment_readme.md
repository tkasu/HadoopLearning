* env file is created with command 'conda env export --file py_environment.yml --no-builds'
* After the creation, following modifications are needed for osx and linux cross platform support:
	* Remove following lines (deps of deps, will be downloaded anyway):
		* libcxxabi=x.x.x
		* libgfortran=x.x.x
		* libcxx=x.x.x
	* Remove 'prefix' line from end of the file
	* TODO create script to remove these manual steps
