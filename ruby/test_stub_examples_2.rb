require File.expand_path(File.dirname(__FILE__)) + '/../test_helper'

class SchedulesControllerTest < ActionController::TestCase
  include LoginTestHelper

  def setup
    super
    @user = Account::User.find(2)
    Barrister::Professional.any_instance.stubs(:claimed_by?).returns(true)
    stub_user(@user)
  end

  def test_routes
    assert_recognizes({:controller => "schedules", :action => "edit", :lawyer_id => "1"}, {:path => "/attorney-edit/1/schedule/edit", :method => :get})
    assert_recognizes({:controller => "schedules", :action => "show", :lawyer_id => "1"}, {:path => "/attorney-edit/1/schedule", :method => :get})
    assert_recognizes({:controller => "schedules", :action => "update", :lawyer_id => "1"}, {:path => "/attorney-edit/1/schedule", :method => :put})
  end

  def test_edit
    external_service = OpenStruct.new(config_value: "https://example.com")
    Solicitor::Client::ProfessionalExternalServices.expects(:where).returns([external_service])
    xhr :get, :edit, lawyer_id: @user.professional.id
    assert assigns(:professional)
    assert_equal assigns(:schedule_form).professional_id, @user.professional.id
    assert_equal assigns(:schedule_form).scheduling_url, "https://example.com"
    assert_response :success
    assert_template :edit
  end

  def test_successful_update
    Profile::ScheduleForm.expects(:find).with(@user.professional.id).returns(nil)
    Solicitor::Client::ProfessionalExternalServices.expects(:create).with({ professional_id: @user.professional.id,
                                                                            service_name: "appointment_scheduling",
                                                                            config_name: "scheduling_url",
                                                                            config_value: "https://scheduling.com" }).returns(true)
    xhr :put, :update, lawyer_id: @user.professional.id, schedule_form: { scheduling_url: "https://scheduling.com" }
    assert_response :success
    assert_template :show
  end


  def test_invalid_update
    Profile::ScheduleForm.expects(:find).with(@user.professional.id).returns(nil)
    Solicitor::Client::ProfessionalExternalServices.expects(:create).never
    xhr :put, :update, lawyer_id: @user.professional.id, schedule_form: { scheduling_url: '' }
    assert assigns(:professional)
    assert_equal assigns(:schedule_form).professional_id, @user.professional.id
    assert_equal assigns(:schedule_form).errors[:scheduling_url], ["The URL you entered is incorrect, please try again with the correct one."]
    assert_template :edit
  end

  def test_show
    professional_schedule_config = Profile::ScheduleForm.new(professional_id: @user.professional.id, scheduling_url: "https://example.com" )
    Profile::ScheduleForm.expects(:find).with(@user.professional.id).returns(professional_schedule_config)
    xhr :get, :show, lawyer_id: @user.professional.id
    assert assigns(:professional)
    assert_equal assigns(:schedule_form).professional_id, @user.professional.id
    assert_equal assigns(:schedule_form).scheduling_url, "https://example.com"
    assert_template :show
  end
end
