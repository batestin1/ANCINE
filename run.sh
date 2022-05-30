#/bash/bin
                        #********************************************************************************#
                        #                                                                                #
                        #                                  нεℓℓσ,вαтεs!                                  #
                        #                                                                                #
                        #   filename: run.sh                                                             #
                        #   created: 2022-05-22                                                          #
                        #   system: Windows                                                              #
                        #   version: 64bit                                                               #
                        #                                       by: Bates <https://github.com/batestin1> #
                        #********************************************************************************#
                        #                           import your librarys below                           #
                        #********************************************************************************#

LOG=/home/bates/repositorio/big_data/one_piece/logs/logs.log
echo "Começando o projeto no dia "{$date}"" >>${LOG}

cd /home/bates/repositorio/big_data/ancine/scripts
python3 /home/bates/repositorio/big_data/ancine/scripts/scriptSourceAncine.py >>${LOG}

python3 /home/bates/repositorio/big_data/ancine/scripts/scriptSourceAncine.py >>${LOG}
python3 /home/bates/repositorio/big_data/ancine/scripts/scriptStaginSilverAncine.py >>${LOG}



echo "Projeto Finalizado" >>${LOG}