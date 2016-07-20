

rm -f ./xin.log

#./build/mvn -DskipTests clean package > xin.log &
./build/mvn -DskipTests package > xin.log &

echo "helloworld!!!"
