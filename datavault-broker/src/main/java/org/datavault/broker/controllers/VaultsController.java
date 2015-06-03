package org.datavault.broker.controllers;

import java.util.List;

import org.datavault.common.model.Vault;
import org.datavault.common.model.Deposit;
import org.datavault.broker.services.VaultsService;

import org.datavault.queue.Sender;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * User: Tom Higgins
 * Date: 11/05/2015
 * Time: 14:29
 */

@RestController
public class VaultsController {
    
    private VaultsService vaultsService;
    private Sender sender;

    public void setVaultsService(VaultsService vaultsService) {
        this.vaultsService = vaultsService;
    }

    public void setSender(Sender sender) {
        this.sender = sender;
    }

    @RequestMapping(value = "/vaults", method = RequestMethod.GET)
    public List<Vault> getVaults() {
        return vaultsService.getVaults();
    }
    
    @RequestMapping(value = "/vaults", method = RequestMethod.POST)
    public Vault addVault(@RequestBody Vault vault) {
        vaultsService.addVault(vault);
        return vault;
    }

    @RequestMapping(value = "/vaults/{vaultid}", method = RequestMethod.GET)
    public Vault getVault(@PathVariable("vaultid") String vaultID) {
        return vaultsService.getVault(vaultID);
    }
    
    @RequestMapping(value = "/vaults/{vaultid}/deposits", method = RequestMethod.GET)
    public List<Deposit> getDeposits(@PathVariable("vaultid") String vaultID) {
        Vault vault = vaultsService.getVault(vaultID);
        return vault.getDeposits();
    }
    
    @RequestMapping(value = "/vaults/{vaultid}/deposits/{depositid}", method = RequestMethod.GET)
    public Deposit getDeposit(@PathVariable("vaultid") String vaultID,
                              @PathVariable("depositid") int depositID) {
        Deposit deposit = vaultsService.getVault(vaultID).getDeposit(depositID);
        return deposit;
    }
    
    @RequestMapping(value = "/vaults/{vaultid}/deposits/{depositid}/status", method = RequestMethod.GET)
    public Deposit.Status getDepositState(@PathVariable("vaultid") String vaultID,
                                          @PathVariable("depositid") int depositID) {
        Deposit deposit = vaultsService.getVault(vaultID).getDeposit(depositID);
        return deposit.getStatus();
    }
    
    @RequestMapping(value = "/vaults/{vaultid}/deposits/{depositid}/status", method = RequestMethod.POST)
    public Deposit setDepositState(@PathVariable("vaultid") String vaultID,
                              @PathVariable("depositid") int depositID,
                              @RequestBody Deposit.Status status) {
        Deposit deposit = vaultsService.getVault(vaultID).getDeposit(depositID);
        deposit.setStatus(status);
        return deposit;
    }
    
    @RequestMapping(value = "/vaults/{vaultid}/deposits/{depositid}/add", method = RequestMethod.POST)
    public Boolean addDepositFiles(@PathVariable("vaultid") String vaultID,
                                   @PathVariable("depositid") int depositID,
                                   @RequestBody List<String> filePaths) {
        
        Deposit deposit = vaultsService.getVault(vaultID).getDeposit(depositID);
        
        try {
            for (String filePath : filePaths) {
                sender.send(filePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        
        // Need to update Deposit object too?
        
        return true;
    }
}
